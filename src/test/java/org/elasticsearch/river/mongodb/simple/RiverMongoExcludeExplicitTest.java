/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.river.mongodb.simple;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.bson.types.ObjectId;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.river.mongodb.RiverMongoDBTestAbstract;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@Test
public class RiverMongoExcludeExplicitTest extends RiverMongoDBTestAbstract {

    private DB mongoDB;
    private DBCollection mongoCollection;

    @Factory(dataProvider = "onlyTokuMX")
    public RiverMongoExcludeExplicitTest(ExecutableType type) {
        super(type);
    }

    @BeforeClass
    public void createDatabase() {
        logger.debug("createDatabase {}", getDatabase());
        try {
            mongoDB = getMongo().getDB(getDatabase());
            mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
            super.createRiver(TEST_MONGODB_RIVER_EXCLUDE_FIELDS_JSON, getRiver(), 3, "[]", getDatabase(), getCollection(), getIndex(), getDatabase());
            logger.info("Start createCollection");
            mongoCollection = mongoDB.createCollection(getCollection(), null);
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
    public void testExcludeUpdates() throws Throwable {
        logger.debug("Start testExcludeUpdate");
        try {
            DBObject dbObject = new BasicDBObject();
            dbObject.put("field-1", 1);
            mongoCollection.insert(dbObject);
            Thread.sleep(wait);

            String id = dbObject.get("_id").toString();
            assertThat(getNode().client().admin().indices().exists(new IndicesExistsRequest(getIndex())).actionGet().isExists(), equalTo(true));
            refreshIndex();

            SearchResponse sr = getNode().client().prepareSearch(getIndex()).setQuery(QueryBuilders.queryString(id).defaultField("_id")).get();
            logger.debug("SearchResponse {}", sr.toString());
            long totalHits = sr.getHits().getTotalHits();
            logger.debug("TotalHits: {}", totalHits);
            assertThat(totalHits, equalTo(1l));

            Map<String, Object> object = sr.getHits().getHits()[0].sourceAsMap();
            assertThat(object.get("field-1").equals(1), equalTo(true));

            // Update Mongo object
            dbObject = mongoCollection.findOne(new BasicDBObject("_id", new ObjectId(id)));

            // Ensure that normal updates work
            DBObject finder = BasicDBObjectBuilder.start("_id", dbObject.get("_id")).get();
            DBObject updates = (DBObject) JSON.parse("{$set: {'field-1': 2}}");
            mongoCollection.update(finder, updates);
            Thread.sleep(wait);

            sr = getNode().client().prepareSearch(getIndex()).setQuery(QueryBuilders.queryString(id).defaultField("_id")).get();
            logger.debug("SearchResponse {}", sr.toString());
            totalHits = sr.getHits().getTotalHits();
            logger.debug("TotalHits: {}", totalHits);
            assertThat(totalHits, equalTo(1l));

            object = sr.getHits().getHits()[0].sourceAsMap();
            assertThat(object.get("field-1").equals(2), equalTo(true));

            // Ensure that skipped updates work
            updates = (DBObject) JSON.parse("{$set: {'field-1': 3}, $unset: {__es_skip: 1}}");
            mongoCollection.update(finder, updates);
            Thread.sleep(wait);

            sr = getNode().client().prepareSearch(getIndex()).setQuery(QueryBuilders.queryString(id).defaultField("_id")).get();
            logger.debug("SearchResponse {}", sr.toString());
            totalHits = sr.getHits().getTotalHits();
            logger.debug("TotalHits: {}", totalHits);
            assertThat(totalHits, equalTo(1l));

            object = sr.getHits().getHits()[0].sourceAsMap();
            logger.debug("Object: {}", object);
            assertThat(object.get("field-1").equals(2), equalTo(true));
        } catch (Throwable t) {
            logger.error("testExcludeFields failed.", t);
            t.printStackTrace();
            throw t;
        }
    }

}
