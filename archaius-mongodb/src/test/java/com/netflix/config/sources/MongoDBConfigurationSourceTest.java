package com.netflix.config.sources;

import com.mongodb.MongoClient;
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
    private static MongodExecutable mongodExe;
    private static MongodProcess mongod;

    @BeforeClass
    public static void Before() throws Exception {
        MongoDBRuntime runtime = MongoDBRuntime.getDefaultInstance();
        mongodExe = runtime.prepare(new MongodConfig(Version.V2_2_0_RC0, 54321, Network.localhostIsIPv6()));
        mongod = mongodExe.start();
        
        //seed some settings
        MongoClient client = new MongoClient("localhost", 54321);
        try{
            client.getDatabase("Settings").getCollection("price").insertMany(Arrays.asList(
                    new Document("_id", "DefaultCurrency").append("value", "USD"),
                    new Document("_id", "Levels").append("value", Arrays.asList("Retail", "Trade"))
            ));
        }finally {
            client.close();
        }
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
        MongoDBConfigurationSource configurationSource = new MongoDBConfigurationSource("mongodb://localhost:54321", "Settings", "price");
        final Map<String, Object> settings = configurationSource.load();

        assertEquals(2, settings.size());
        assertEquals("USD", settings.get("DefaultCurrency"));

        List<String> levels = (List<String>) settings.get("Levels");
        assertEquals("[Retail, Trade]", levels.toString());
    }
}