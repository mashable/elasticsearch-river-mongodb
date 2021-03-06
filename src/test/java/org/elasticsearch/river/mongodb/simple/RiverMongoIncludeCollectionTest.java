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

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.river.mongodb.RiverMongoDBTestAbstract;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;

@Test
public class RiverMongoIncludeCollectionTest extends RiverMongoDBTestAbstract {

    private static final String TEST_SIMPLE_MONGODB_RIVER_INCLUDE_COLLECTION_JSON = "/org/elasticsearch/river/mongodb/simple/test-simple-mongodb-river-include-collection.json";
    private DB mongoDB;
    private DBCollection mongoCollection;
    private String includeCollectionOption = "mycollection";

    @Factory(dataProvider = "allMongoExecutableTypes")
    public RiverMongoIncludeCollectionTest(ExecutableType type) {
        super(type);
    }

    @BeforeClass
    public void createDatabase() {
        logger.debug("createDatabase {}", getDatabase());
        try {
            mongoDB = getMongo().getDB(getDatabase());
            mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
            super.createRiver(
                    TEST_SIMPLE_MONGODB_RIVER_INCLUDE_COLLECTION_JSON, getRiver(), 3, includeCollectionOption,
                    getDatabase(), getCollection(),  getIndex(), getDatabase());

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
    public void testIncludeCollection() throws Throwable {
        logger.debug("Start testIncludeCollection");
        try {
            String mongoDocument = copyToStringFromClasspath(TEST_SIMPLE_MONGODB_DOCUMENT_JSON);
            DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
            mongoCollection.insert(dbObject);
            waitForRiverReplication();

            assertThat(getNode().client().admin().indices().exists(new IndicesExistsRequest(getIndex())).actionGet().isExists(),
                    equalTo(true));
            assertThat(getNode().client().admin().indices().prepareTypesExists(getIndex()).setTypes(getDatabase()).execute().actionGet()
                    .isExists(), equalTo(true));

            String collectionName = mongoCollection.getName();

            CountResponse countResponse = getNode().client()
                    .prepareCount(getIndex()).setQuery(QueryBuilders.queryString(collectionName).defaultField(includeCollectionOption)).get();
            assertThat(countResponse.getCount(), equalTo(1L));
        } catch (Throwable t) {
            logger.error("testIncludeCollection failed.", t);
            t.printStackTrace();
            throw t;
        }
    }
}
