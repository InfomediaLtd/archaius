/**
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.config.sources;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.netflix.config.PollResult;
import com.netflix.config.PolledConfigurationSource;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * Simple MongoDB backed source of config properties. Assuming you have collection(s) with a
 * document per setting where the _id is a string key and a field 'value' stores the value.
 *
 * @author lthompson
 */
public class MongoDBConfigurationSource implements PolledConfigurationSource {
    private static Logger log = LoggerFactory
            .getLogger(MongoDBConfigurationSource.class);

    private final String databaseName;
    private final String collectionName;
    private final MongoClient mongoClient;

    public MongoDBConfigurationSource(String mongoURI, String databaseName, String collectionName) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;

        mongoClient = new MongoClient(new MongoClientURI(mongoURI));
    }

    @Override
    public PollResult poll(boolean initial, Object checkPoint) throws Exception {
        Map<String, Object> map = load();
        return PollResult.createFull(map);
    }

    /**
     * Returns a <code>Map<String, Object></code> of properties stored in the
     * database.
     *
     * @throws Exception
     */
    synchronized Map<String, Object> load() throws Exception {
        Map<String, Object> map = new HashMap<String, Object>();

        try {
            final MongoDatabase database = mongoClient.getDatabase(databaseName);
            final MongoCollection<Document> collection = database.getCollection(collectionName);
            final FindIterable<Document> documents = collection.find();
            for (Document document : documents) {
                final Object id = document.get("_id");
                if (id != null && document.containsKey("value")) {
                    final Object value = document.get("value");
                    map.put(id.toString(), value);
                }
            }

        } catch (Exception e) {
            log.error("Failed to load archaius settings from MongoDB.", e);
            throw e;
        }
        return map;
    }

}
