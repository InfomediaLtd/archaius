package com.netflix.config.sources;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.netflix.config.*;
import de.flapdoodle.embedmongo.MongoDBRuntime;
import de.flapdoodle.embedmongo.MongodExecutable;
import de.flapdoodle.embedmongo.MongodProcess;
import de.flapdoodle.embedmongo.config.MongodConfig;
import de.flapdoodle.embedmongo.distribution.Version;
import de.flapdoodle.embedmongo.runtime.Network;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by lthompson on 14/08/15.
 */
public class MongoDBConfigurationSourceTest {
    public static final int MONGO_PORT = 32321;
    private static MongodExecutable mongodExe;
    private static MongodProcess mongod;

    @BeforeClass
    public static void Before() throws Exception {
        MongoDBRuntime runtime = MongoDBRuntime.getDefaultInstance();
        mongodExe = runtime.prepare(new MongodConfig(Version.V2_2_0_RC0, MONGO_PORT, Network.localhostIsIPv6()));
        mongod = mongodExe.start();
        
        updateSettings("USD");
    }

    @AfterClass
    public static void After() throws Exception {
        if (mongod != null) {
            mongod.stop();
            mongodExe.cleanup();
        }
    }

    @Test
    public void testLoad() throws Exception {
        MongoDBConfigurationSource configurationSource = new MongoDBConfigurationSource("mongodb://localhost:" + MONGO_PORT, "Settings", "price");
        final Map<String, Object> settings = configurationSource.load();

        assertEquals(2, settings.size());
        assertEquals("USD", settings.get("DefaultCurrency"));

        List<String> levels = (List<String>) settings.get("Levels");
        assertEquals("[Retail, Trade]", levels.toString());
    }

    //@Test
    public void testDynamicPropertyUpdate() throws Exception {
        MongoDBConfigurationSource source = new MongoDBConfigurationSource("mongodb://localhost:" + MONGO_PORT, "Settings", "price");
        FixedDelayPollingScheduler scheduler = new FixedDelayPollingScheduler(0, 1000, false);
        DynamicConfiguration dynamicConfig = new DynamicConfiguration(source, scheduler);
        ConfigurationManager.loadPropertiesFromConfiguration(dynamicConfig);

        DynamicStringProperty defaultCurrency = DynamicPropertyFactory.getInstance().getStringProperty("DefaultCurrency","");

        assertEquals("USD", defaultCurrency.get());

        updateSettings("AUD");
        Thread.sleep(5000);

        assertEquals("AUD", defaultCurrency.get());
    }



    private static void updateSettings(String defaultCurrency) {
        MongoClient client = new MongoClient("localhost", MONGO_PORT);
        try{
            final MongoCollection<Document> collection = client.getDatabase("Settings").getCollection("price");

            collection.findOneAndReplace(
                    new Document("_id", "DefaultCurrency"),
                    new Document("_id", "DefaultCurrency").append("value", defaultCurrency),
                    new FindOneAndReplaceOptions().upsert(true)
                    );

            collection.findOneAndReplace(
                    new Document("_id", "Levels"),
                    new Document("_id", "Levels").append("value", Arrays.asList("Retail", "Trade")),
                    new FindOneAndReplaceOptions().upsert(true)
            );

        }finally {
            client.close();
        }
    }
}