package org.elasticsearch.river.mongodb.tokumx;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;
import org.bson.types.ObjectId;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

public class RiverTokuMXOplogPKTest extends RiverTokuMXTestAbstract {
    private DB mongoDB;
    private DBCollection mongoCollection;

    @BeforeClass
    public void createDatabase() {
        logger.debug("createDatabase {}", getDatabase());
        try {
            mongoDB = getMongo().getDB(getDatabase());
            mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
            super.createRiver(TEST_MONGODB_RIVER_SIMPLE_JSON);
            logger.info("Start createCollection");
            DBObject options = (DBObject) JSON.parse("{primaryKey: {customKey: 1, _id: 1}}");
            this.mongoCollection = mongoDB.createCollection(getCollection(), options);
            Assert.assertNotNull(mongoCollection);
        } catch (Throwable t) {
            logger.error("createDatabase failed.", t);
        }
    }

    @AfterClass
    public void cleanUp() {
        super.deleteRiver();
        logger.info("Drop database " + mongoDB.getName());
        mongoDB.dropDatabase();
    }

    @Test
    public void testOplogRefs() throws InterruptedException {
        BasicDBObject dbObject = (BasicDBObject) JSON.parse("{customKey: 1}");
        ObjectId id = new ObjectId();
        dbObject.append("_id", id);
        mongoCollection.insert(dbObject, WriteConcern.REPLICAS_SAFE);

        BasicDBObject key = (BasicDBObject) JSON.parse("{customKey: 1}");
        key.append("_id", id);
        DBObject update = (DBObject) JSON.parse("{$set: {value: 1}}");
        mongoCollection.update(key, update);

        Thread.sleep(wait);

        ActionFuture<IndicesExistsResponse> response = getNode().client().admin().indices().exists(new IndicesExistsRequest(getIndex()));
        assertThat(response.actionGet().isExists(), equalTo(true));

        refreshIndex();
        SearchRequest search = getNode().client().prepareSearch(getIndex()).setQuery(QueryBuilders.queryString("1").defaultField("value"))
                .request();

        SearchResponse searchResponse = getNode().client().search(search).actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));

        Map<String, Object> hit = searchResponse.getHits().getAt(0).getSource();

        Integer inc = (Integer) hit.get("customKey");
        assertThat(inc, equalTo(1));
    }
}
