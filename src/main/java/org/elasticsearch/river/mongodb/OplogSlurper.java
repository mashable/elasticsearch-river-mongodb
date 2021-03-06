package org.elasticsearch.river.mongodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.river.mongodb.util.MongoDBHelper;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.QueryOperators;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSFile;
import com.mongodb.util.JSONSerializers;

class OplogSlurper implements Runnable {

    class SlurperException extends Exception {

        private static final long serialVersionUID = 1L;

        SlurperException(String message) {
            super(message);
        }
    }

    private static final ESLogger logger = ESLoggerFactory.getLogger(OplogSlurper.class.getName());

    private final MongoDBRiverDefinition definition;
    private final SharedContext context;
    private final BasicDBObject findKeys;
    private final String gridfsOplogNamespace;
    private final String cmdOplogNamespace;
    private final ImmutableList<String> oplogOperations = ImmutableList.of(MongoDBRiver.OPLOG_DELETE_OPERATION,
            MongoDBRiver.OPLOG_UPDATE_ROW_OPERATION, // from TokuMX
            MongoDBRiver.OPLOG_UPDATE_OPERATION, MongoDBRiver.OPLOG_INSERT_OPERATION, MongoDBRiver.OPLOG_COMMAND_OPERATION);
    private final Client esClient;
    private final MongoClient mongoClusterClient;
    private final MongoClient mongoShardClient;
    private Timestamp<?> timestamp;
    private final DB slurpedDb;
    private final DB oplogDb;
    private final DBCollection oplogCollection, oplogRefsCollection;
    private final AtomicLong totalDocuments = new AtomicLong();
    private HashMap<String, ArrayList<String>> pkCache;

    public OplogSlurper(Timestamp<?> timestamp, MongoClient mongoClusterClient, MongoClient mongoShardClient, MongoDBRiverDefinition definition, SharedContext context, Client esClient) {
        this.timestamp = timestamp;
        this.definition = definition;
        this.context = context;
        this.esClient = esClient;
        this.mongoClusterClient = mongoClusterClient;
        this.mongoShardClient = mongoShardClient;
        this.findKeys = new BasicDBObject();
        this.gridfsOplogNamespace = definition.getMongoOplogNamespace() + MongoDBRiver.GRIDFS_FILES_SUFFIX;
        this.cmdOplogNamespace = definition.getMongoDb() + "." + MongoDBRiver.OPLOG_NAMESPACE_COMMAND;
        this.pkCache = new HashMap<String, ArrayList<String>>();
        if (definition.getExcludeFields() != null) {
            for (String key : definition.getExcludeFields()) {
                findKeys.put(key, 0);
            }
        } else if (definition.getIncludeFields() != null) {
            for (String key : definition.getIncludeFields()) {
                findKeys.put(key, 1);
            }
        }
        this.oplogDb = mongoShardClient.getDB(MongoDBRiver.MONGODB_LOCAL_DATABASE);
        this.oplogCollection = oplogDb.getCollection(MongoDBRiver.OPLOG_COLLECTION);
        this.oplogRefsCollection = oplogDb.getCollection(MongoDBRiver.OPLOG_REFS_COLLECTION);
        this.slurpedDb = mongoShardClient.getDB(definition.getMongoDb());
    }

    @Override
    public void run() {
        while (context.getStatus() == Status.RUNNING) {
            try {        
                // Slurp from oplog
                DBCursor cursor = null;
                try {
                    cursor = oplogCursor(timestamp);
                    if (cursor == null) {
                        cursor = processFullOplog();
                    }
                    while (cursor.hasNext()) {
                        DBObject item = cursor.next();
                        // TokuMX secondaries can have ops in the oplog that
                        // have not yet been applied
                        // We need to wait until they have been applied before
                        // processing them
                        Object applied = item.get("a");
                        if (applied != null && !applied.equals(Boolean.TRUE)) {
                            logger.debug("Encountered oplog entry with a:false, ts:" + item.get("ts"));
                            break;
                        }
                        timestamp = processOplogEntry(item, timestamp);
                    }
                    logger.debug("Before waiting for 500 ms");
                    Thread.sleep(500);
                } finally {
                    if (cursor != null) {
                        logger.trace("Closing oplog cursor");
                        cursor.close();
                    }
                }
            } catch (SlurperException e) {
                logger.error("Exception in slurper", e);
                Thread.currentThread().interrupt();
                break;
            } catch (MongoInterruptedException | InterruptedException e) {
                logger.info("river-mongodb slurper interrupted");
                Thread.currentThread().interrupt();
                break;
            } catch (MongoSocketException | MongoTimeoutException | MongoCursorNotFoundException e) {
                logger.info("Oplog tailing - {} - {}. Will retry.", e.getClass().getSimpleName(), e.getMessage());
                logger.debug("Total documents inserted so far by river {}: {}", definition.getRiverName(), totalDocuments.get());
                try {
                    Thread.sleep(MongoDBRiver.MONGODB_RETRY_ERROR_DELAY_MS);
                } catch (InterruptedException iEx) {
                    logger.info("river-mongodb slurper interrupted");
                    Thread.currentThread().interrupt();
                    break;
                }
            } catch (Exception e) {
                logger.error("Exception while looping in cursor", e);
                Thread.currentThread().interrupt();
                break;
            }
        }
        logger.info("Slurper is stopping. River has status {}", context.getStatus());
    }

    protected boolean riverHasIndexedFromOplog() {
        return MongoDBRiver.getLastTimestamp(esClient, definition) != null;
    }

    protected boolean isIndexEmpty() {
        return MongoDBRiver.getIndexCount(esClient, definition) == 0;
    }

    private Timestamp<?> getCurrentOplogTimestamp() {
        try (DBCursor cursor = oplogCollection.find().sort(new BasicDBObject(MongoDBRiver.INSERTION_ORDER_KEY, -1)).limit(1)) {
            return Timestamp.on(cursor.next());
        }
    }

    private DBCursor processFullOplog() throws InterruptedException, SlurperException {
        Timestamp<?> currentTimestamp = getCurrentOplogTimestamp();
        return oplogCursor(currentTimestamp);
    }

    private Timestamp<?> processOplogEntry(final DBObject entry, final Timestamp<?> startTimestamp) throws InterruptedException, SlurperException {
        return processOplogEntry(entry, startTimestamp, Timestamp.on(entry));
    }

    private Timestamp<?> processOplogEntry(final DBObject entry, final Timestamp<?> startTimestamp, Timestamp<?> oplogTimestamp) throws InterruptedException, SlurperException {
        if(entry.containsField(MongoDBRiver.OPLOG_REF)) {
            return processOplogRefs(entry, startTimestamp, oplogTimestamp);
        } else {
            Object ops = entry.get(MongoDBRiver.OPLOG_OPS);
            if(ops != null) {
                for (BasicDBObject op : (List<BasicDBObject>) ops) {
                    oplogTimestamp = processSingleOp(op, startTimestamp, oplogTimestamp);
                }
            } else {
                oplogTimestamp = processSingleOp(entry, startTimestamp, oplogTimestamp);
            }
            return oplogTimestamp;
        }
    }

    private Timestamp<?> processSingleOp(final DBObject entry, final Timestamp<?> startTimestamp, final Timestamp<?> oplogTimestamp) throws InterruptedException {
        if (!isValidOplogEntry(entry, startTimestamp, oplogTimestamp)) {
            return startTimestamp;
        }
        Operation operation = Operation.fromString(entry.get(MongoDBRiver.OPLOG_OPERATION).toString());
        String namespace = entry.get(MongoDBRiver.OPLOG_NAMESPACE).toString();
        String collection = null;
        DBObject object = (DBObject) entry.get(MongoDBRiver.OPLOG_OBJECT);

        if (definition.isImportAllCollections()) {
            if (namespace.startsWith(definition.getMongoDb()) && !namespace.equals(cmdOplogNamespace)) {
                collection = getCollectionFromNamespace(namespace);
            }
        } else {
            collection = definition.getMongoCollection();
        }

        if (namespace.equals(cmdOplogNamespace)) {
            if (object.containsField(MongoDBRiver.OPLOG_DROP_COMMAND_OPERATION)) {
                operation = Operation.DROP_COLLECTION;
                if (definition.isImportAllCollections()) {
                    collection = object.get(MongoDBRiver.OPLOG_DROP_COMMAND_OPERATION).toString();
                    if (collection.startsWith("tmp.mr.")) {
                        return startTimestamp;
                    }
                }
            }
            if (object.containsField(MongoDBRiver.OPLOG_DROP_DATABASE_COMMAND_OPERATION)) {
                operation = Operation.DROP_DATABASE;
            }
        }

        if(logger.isTraceEnabled())
            logger.trace("namespace: {} - operation: {}", namespace, operation);

        if (namespace.equals(MongoDBRiver.OPLOG_ADMIN_COMMAND) && operation == Operation.COMMAND) {
            processAdminCommandOplogEntry(entry);
            return startTimestamp;
        }

        String objectId = getObjectIdFromOplogEntry(entry);
        if (operation == Operation.DELETE) {
            // Include only _id in data, as vanilla MongoDB does, so
            // transformation scripts won't be broken by Toku
            if (object.containsField(MongoDBRiver.MONGODB_ID_FIELD)) {
                if (object.keySet().size() > 1) {
                    entry.put(MongoDBRiver.OPLOG_OBJECT, object = new BasicDBObject(MongoDBRiver.MONGODB_ID_FIELD, objectId));
                }
            } else {
                throw new NullPointerException(MongoDBRiver.MONGODB_ID_FIELD);
            }
        }

        if (definition.isMongoGridFS() && namespace.endsWith(MongoDBRiver.GRIDFS_FILES_SUFFIX)
                && (operation == Operation.INSERT || operation == Operation.UPDATE || operation == Operation.UPDATE_ROW)) {
            if (objectId == null) {
                throw new NullPointerException(MongoDBRiver.MONGODB_ID_FIELD);
            }
            GridFS grid = new GridFS(mongoShardClient.getDB(definition.getMongoDb()), collection);
            GridFSDBFile file = grid.findOne(new ObjectId(objectId));
            if (file != null) {
                logger.trace("Caught file: {} - {}", file.getId(), file.getFilename());
                object = file;
            } else {
                logger.error("Cannot find file from id: {}", objectId);
            }
        }

        if (object instanceof GridFSDBFile) {
            if (objectId == null) {
                throw new NullPointerException(MongoDBRiver.MONGODB_ID_FIELD);
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Add attachment: {}", objectId);
            }
            addToStream(operation, oplogTimestamp, applyFieldFilter(object), collection);
        } else {
            BasicDBObject update;
            switch (operation) {
                case INSERT:
                    addInsertToStream(oplogTimestamp, applyFieldFilter(object), collection);
                    break;
                case UPDATE:
                case UPDATE_ROW:
                    update = (BasicDBObject)entry.get(MongoDBRiver.OPLOG_UPDATE);
                    // Under tokumx, o2 will be null for UPDATE_ROW. Instead we want to query for the doc ID
                    if (update == null && object == null) {
                        // tokumx 2.0.0 UPDATE_ROW; pk and m are present, o and o2 are not.
                        BasicDBObject pk = (BasicDBObject)entry.get("pk");
                        if (pk != null) {
                            BasicDBObject selector = mapPKFields(collection, pk);
                            if (selector != null) {
                                addQueryToStream(operation, oplogTimestamp, selector, collection);
                                break;
                            }
                        }
                    } else if (update == null) {
                        ObjectId updateID = ((BasicDBObject)object).getObjectId(MongoDBRiver.MONGODB_ID_FIELD, null);
                        if (updateID != null) {
                            update = new BasicDBObject(MongoDBRiver.MONGODB_ID_FIELD, updateID);
                            addQueryToStream(operation, oplogTimestamp, update, collection);
                        }
                    } else if (definition.isTokumx) {
                        // tokumx provides a postimage, we can just save it
                        addToStream(operation, oplogTimestamp, applyFieldFilter(update), collection);
                    } else {
                        // mongo doesn't provide the postimage, so we have to query on the update
                        addQueryToStream(operation, oplogTimestamp, update, collection);
                    }
                    break;
                default:
                    addToStream(operation, oplogTimestamp, applyFieldFilter(object), collection);
                    break;
            }
        }
        return oplogTimestamp;
    }


    // Tokumx doesn't provide key names in the `pk` field. We'll have to map them from the collection ourselves.
    private BasicDBObject mapPKFields(String namespace, BasicDBObject pk) {
        ArrayList<String> map = getPKMapFor(namespace);
        if (map == null) return null;

        BasicDBObject selector = new BasicDBObject();
        int index = map.size() - pk.size();
        for(Map.Entry<String, Object> entry : pk.entrySet()) {
            selector.append(map.get(index), entry.getValue());
            index++;
        }
        return selector;
    }

    private ArrayList<String> getPKMapFor(String namespace) {
        if(!pkCache.containsKey(namespace)) {
            List<DBObject> indexes = slurpedDb.getCollection(namespace).getIndexInfo();
            BasicDBObject idKey = null;
            BasicDBObject pkKey = null;
            for(DBObject idx : indexes) {
                if(idx.get("name").equals("primaryKey")) {
                    pkKey = (BasicDBObject)idx.get("key");
                } else if (idx.get("name").equals("_id_")) {
                    idKey = (BasicDBObject)idx.get("key");
                }
            }
            ArrayList<String> keys = new ArrayList<String>();
            if (pkKey != null) {
                for (Map.Entry<String, Object> entry : pkKey.entrySet()) {
                    keys.add(entry.getKey());
                }
            } else if (idKey != null) {
                for (Map.Entry<String, Object> entry : idKey.entrySet()) {
                    keys.add(entry.getKey());
                }
            }
            if (keys.size() != 0) {
                pkCache.put(namespace, keys);
            }
            logger.debug("pkcache for {}: {}", namespace, keys);
        }
        if(pkCache.containsKey(namespace)) {
            return pkCache.get(namespace);
        } else {
            return null;
        }
    }

    private Timestamp<?> processOplogRefs(final DBObject entry, final Timestamp<?> timestamp, final Timestamp<?> oplogTimestamp) throws InterruptedException, SlurperException {
        ObjectId ref = (ObjectId) entry.get(MongoDBRiver.OPLOG_REF);
        long seq = 0;
        if(ref != null) {
            // Find the refs matching this oplog entry and update the docs they touch
            // db.oplog.refs.find({_id: {$gt: {oid: id_from_oplog, seq: 0}}})
            // db.oplog.refs.find({"_id.oid": ObjectId("5564b20d996b5d1f5a753633"), "_id.seq": {$gt: 0} })
            while(true) {
                BasicDBObject selector = new BasicDBObject("_id.".concat(MongoDBRiver.MONGODB_OID_FIELD), ref).append("_id.".concat(MongoDBRiver.MONGODB_SEQ_FIELD), new BasicDBObject(QueryOperators.GT, seq));
                // BasicDBObject query = new BasicDBObject(MongoDBRiver.MONGODB_ID_FIELD, new BasicDBObject(QueryOperators.GT, selector));
                BasicDBObject refResult = (BasicDBObject)oplogRefsCollection.findOne(selector);
                // logger.debug("Finding refs for {}", query);
                if(refResult == null) {
                    logger.debug("Got no refResult for {}, breaking...", selector);
                    break;
                }

                BasicDBObject refId = (BasicDBObject)refResult.get(MongoDBRiver.MONGODB_ID_FIELD);
                ObjectId refOid = (ObjectId)refId.get(MongoDBRiver.MONGODB_OID_FIELD);
                if( !refOid.equals(ref) ) {
                    // logger.debug("{} != {}, breaking oplog ref loop...", ref, refOid);
                    break;
                }

                logger.debug("Processing oplog.refs entry: {} seq {}", ref, seq);

                seq = refId.getLong(MongoDBRiver.MONGODB_SEQ_FIELD);
                processOplogEntry(refResult, timestamp, oplogTimestamp);
            }
            return timestamp;
        } else {
            throw new SlurperException("Invalid oplog entry - namespace is null, but ref field is missing");
        }
    }

    private void processAdminCommandOplogEntry(final DBObject entry) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("processAdminCommandOplogEntry - [{}]", entry);
        }
        DBObject object = (DBObject) entry.get(MongoDBRiver.OPLOG_OBJECT);
        if (definition.isImportAllCollections()) {
            if (object.containsField(MongoDBRiver.OPLOG_RENAME_COLLECTION_COMMAND_OPERATION) && object.containsField(MongoDBRiver.OPLOG_TO)) {
                String to = object.get(MongoDBRiver.OPLOG_TO).toString();
                if (to.startsWith(definition.getMongoDb())) {
                    String newCollection = getCollectionFromNamespace(to);
                    DBCollection coll = slurpedDb.getCollection(newCollection);
                    CollectionSlurper importer = new CollectionSlurper(mongoClusterClient, definition, context, esClient);
                    importer.importCollection(coll, timestamp);
                }
            }
        }
    }

    private String getCollectionFromNamespace(String namespace) {
        if (namespace.startsWith(definition.getMongoDb() + '.')) {
            return namespace.substring(definition.getMongoDb().length() + 1);
        }
        logger.error("Cannot get collection from namespace [{}]", namespace);
        return null;
    }

    private boolean isValidOplogEntry(final DBObject entry, final Timestamp<?> startTimestamp, final Timestamp<?> oplogTimestamp) {
        if (!entry.containsField(MongoDBRiver.OPLOG_OPERATION)) {
            logger.trace("[Empty Oplog Entry] - can be ignored. {}", JSONSerializers.getStrict().serialize(entry));
            return false;
        }
        if (MongoDBRiver.OPLOG_NOOP_OPERATION.equals(entry.get(MongoDBRiver.OPLOG_OPERATION))) {
            logger.trace("[No-op Oplog Entry] - can be ignored. {}", JSONSerializers.getStrict().serialize(entry));
            return false;
        }
        String namespace = (String) entry.get(MongoDBRiver.OPLOG_NAMESPACE);
        // Initial support for sharded collection -
        // https://jira.mongodb.org/browse/SERVER-4333
        // Not interested in operation from migration or sharding
        if (entry.containsField(MongoDBRiver.OPLOG_FROM_MIGRATE) && ((BasicBSONObject) entry).getBoolean(MongoDBRiver.OPLOG_FROM_MIGRATE)) {
            logger.trace("[Invalid Oplog Entry] - from migration or sharding operation. Can be ignored. {}", JSONSerializers.getStrict().serialize(entry));
            return false;
        }
        // Not interested by chunks - skip all
        if (namespace.endsWith(MongoDBRiver.GRIDFS_CHUNKS_SUFFIX)) {
            return false;
        }

        if (startTimestamp != null) {
            if (Timestamp.compare(oplogTimestamp, startTimestamp) < 0) {
                logger.error("[Invalid Oplog Entry] - entry timestamp [{}] before startTimestamp [{}]",
                        JSONSerializers.getStrict().serialize(entry), startTimestamp);
                return false;
            }
        }

        boolean validNamespace = false;
        if (definition.isMongoGridFS()) {
            validNamespace = gridfsOplogNamespace.equals(namespace);
        } else {
            if (definition.isImportAllCollections()) {
                // Skip temp entry generated by map / reduce
                if (namespace.startsWith(definition.getMongoDb()) && !namespace.startsWith(definition.getMongoDb() + ".tmp.mr")) {
                    validNamespace = true;
                }
            } else {
                if (definition.getMongoOplogNamespace().equals(namespace)) {
                    validNamespace = true;
                }
            }
            if (cmdOplogNamespace.equals(namespace)) {
                validNamespace = true;
            }

            if (MongoDBRiver.OPLOG_ADMIN_COMMAND.equals(namespace)) {
                validNamespace = true;
            }
        }
        if (!validNamespace) {
            // logger.trace("[Invalid Oplog Entry] - namespace [{}] is not valid", namespace);
            return false;
        }
        String operation = (String) entry.get(MongoDBRiver.OPLOG_OPERATION);
        if (!oplogOperations.contains(operation)) {
            // logger.trace("[Invalid Oplog Entry] - operation [{}] is not valid", operation);
            return false;
        }

        if(explicitSkip(entry)) {
            return false;
        }

        // TODO: implement a better solution
        if (definition.getMongoOplogFilter() != null) {
            DBObject object = (DBObject) entry.get(MongoDBRiver.OPLOG_OBJECT);
            BasicDBObject filter = definition.getMongoOplogFilter();
            if (!filterMatch(filter, object)) {
                logger.trace("[Invalid Oplog Entry] - filter [{}] does not match object [{}]", filter, object);
                return false;
            }
        }
        return true;
    }

    private boolean filterMatch(DBObject filter, DBObject object) {
        for (String key : filter.keySet()) {
            if (!object.containsField(key)) {
                return false;
            }
            if (!filter.get(key).equals(object.get(key))) {
                return false;
            }
        }
        return true;
    }

    private boolean explicitSkip(DBObject object) {
        if(object.containsField(MongoDBRiver.OPLOG_MODS)) {
            DBObject modifiers = (DBObject)object.get(MongoDBRiver.OPLOG_MODS);
            if (modifiers.containsField(MongoDBRiver.OP_UNSET)) {
                DBObject unsets = (DBObject)modifiers.get(MongoDBRiver.OP_UNSET);
                if(unsets.containsField(MongoDBRiver.OP_MAGIC_SKIP)) {
                    return true;
                }
            }
        }
        return false;
    }

    private DBObject applyFieldFilter(DBObject object) {
        if (object instanceof GridFSFile) {
            GridFSFile file = (GridFSFile) object;
            DBObject metadata = file.getMetaData();
            if (metadata != null) {
                file.setMetaData(applyFieldFilter(metadata));
            }
        } else {
            object = MongoDBHelper.applyExcludeFields(object, definition.getExcludeFields());
            object = MongoDBHelper.applyIncludeFields(object, definition.getIncludeFields());
        }
        return object;
    }

    /*
     * Extract "_id" from "o" if it fails try to extract from "o2"
     */
    private String getObjectIdFromOplogEntry(DBObject entry) {
        if (entry.containsField(MongoDBRiver.OPLOG_OBJECT)) {
            DBObject object = (DBObject) entry.get(MongoDBRiver.OPLOG_OBJECT);
            if (object.containsField(MongoDBRiver.MONGODB_ID_FIELD)) {
                return object.get(MongoDBRiver.MONGODB_ID_FIELD).toString();
            }
        }
        if (entry.containsField(MongoDBRiver.OPLOG_UPDATE)) {
            DBObject object = (DBObject) entry.get(MongoDBRiver.OPLOG_UPDATE);
            if (object.containsField(MongoDBRiver.MONGODB_ID_FIELD)) {
                return object.get(MongoDBRiver.MONGODB_ID_FIELD).toString();
            }
        }
        return null;
    }

    private DBCursor oplogCursor(final Timestamp<?> time) throws SlurperException {
        DBObject indexFilter = time.getOplogFilter();
        if (indexFilter == null) {
            return null;
        }

        int options = Bytes.QUERYOPTION_TAILABLE | Bytes.QUERYOPTION_AWAITDATA | Bytes.QUERYOPTION_NOTIMEOUT
        // Using OPLOGREPLAY to improve performance:
        // https://jira.mongodb.org/browse/JAVA-771
                | Bytes.QUERYOPTION_OPLOGREPLAY;

        DBCursor cursor = oplogCollection.find(indexFilter).setOptions(options);

        // Toku sometimes gets stuck without this hint:
        if (indexFilter.containsField(MongoDBRiver.MONGODB_ID_FIELD)) {
            cursor = cursor.hint("_id_");
        }
        isRiverStale(cursor, time);
        return cursor;
    }

    private void isRiverStale(DBCursor cursor, Timestamp<?> time) throws SlurperException {
        if (cursor == null || time == null) {
            return;
        }
        if (definition.getInitialTimestamp() != null && time.equals(definition.getInitialTimestamp())) {
            return;
        }
        DBObject entry = cursor.next();
        Timestamp<?> oplogTimestamp = Timestamp.on(entry);
        if (!time.equals(oplogTimestamp)) {
            MongoDBRiverHelper.setRiverStatus(esClient, definition.getRiverName(), Status.RIVER_STALE);
            throw new SlurperException("River out of sync with oplog.rs collection");
        }
    }

    private void addQueryToStream(final Operation operation, final Timestamp<?> currentTimestamp, final DBObject update,
            final String collection) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("addQueryToStream - operation [{}], currentTimestamp [{}], update [{}]", operation, currentTimestamp, update);
        }

        if (collection == null) {
            for (String name : slurpedDb.getCollectionNames()) {
                DBCollection slurpedCollection = slurpedDb.getCollection(name);
                addQueryToStream(operation, currentTimestamp, update, name, slurpedCollection);
            }
        } else {
            DBCollection slurpedCollection = slurpedDb.getCollection(collection);
            addQueryToStream(operation, currentTimestamp, update, collection, slurpedCollection);
        }
    }

    private void addQueryToStream(final Operation operation, final Timestamp<?> currentTimestamp, final DBObject update,
                final String collection, final DBCollection slurpedCollection) throws InterruptedException {
        DBObject item = slurpedCollection.findOne(update, findKeys);
        if(item != null) {
            addToStream(operation, currentTimestamp, item, collection);
        }
    }

    private String addInsertToStream(final Timestamp<?> currentTimestamp, final DBObject data, final String collection)
            throws InterruptedException {
        totalDocuments.incrementAndGet();
        addToStream(Operation.INSERT, currentTimestamp, data, collection);
        if (data == null) {
            return null;
        } else {
            return data.containsField(MongoDBRiver.MONGODB_ID_FIELD) ? data.get(MongoDBRiver.MONGODB_ID_FIELD).toString() : null;
        }
    }

    private void addToStream(final Operation operation, final Timestamp<?> currentTimestamp, final DBObject data, final String collection)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            String dataString = data.toString();
            if (dataString.length() > 400) {
                logger.trace("addToStream - operation [{}], currentTimestamp [{}], data (_id:[{}], serialized length:{}), collection [{}]",
                        operation, currentTimestamp, data.get("_id"), dataString.length(), collection);
            } else {
                logger.trace("addToStream - operation [{}], currentTimestamp [{}], data [{}], collection [{}]",
                        operation, currentTimestamp, dataString, collection);
            }
        }

        if (operation == Operation.DROP_DATABASE) {
            logger.info("addToStream - Operation.DROP_DATABASE, currentTimestamp [{}], data [{}], collection [{}]",
                    currentTimestamp, data, collection);
            if (definition.isImportAllCollections()) {
                for (String name : slurpedDb.getCollectionNames()) {
                    logger.info("addToStream - isImportAllCollections - Operation.DROP_DATABASE, currentTimestamp [{}], data [{}], collection [{}]",
                            currentTimestamp, data, name);
                    context.getStream().put(new MongoDBRiver.QueueEntry(currentTimestamp, Operation.DROP_COLLECTION, data, name));
                }
            } else {
                context.getStream().put(new MongoDBRiver.QueueEntry(currentTimestamp, Operation.DROP_COLLECTION, data, collection));
            }
        } else {
            context.getStream().put(new MongoDBRiver.QueueEntry(currentTimestamp, operation, data, collection));
        }
    }

}
