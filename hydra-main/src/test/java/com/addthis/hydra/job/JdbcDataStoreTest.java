package com.addthis.hydra.job;

import java.io.File;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.addthis.basis.util.Files;

import com.addthis.bark.ZkUtil;
import com.addthis.hydra.job.store.H2DataStore;
import com.addthis.hydra.job.store.MysqlDataStore;
import com.addthis.hydra.job.store.PostgresqlDataStore;
import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.hydra.job.store.ZookeeperDataStore;
import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JdbcDataStoreTest {

    File tempDir;
    /**
     * A big string to use as a test value. Constructed as a JSON map in order to simulate the things Spawn typically stores.
     */
    private final static String bigJsonString;
    private final static int postgresPort = 5432;
    private final static int mysqlPort = 3306;
    private final static Properties testProperties = new Properties();

    static {
        Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARNING);
        int bigStringKeys = 8000;
        JSONObject obj = new JSONObject();
        try {
            for (int i=0; i<bigStringKeys; i++) {
                obj.put("key" + i, "val" + i);
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        bigJsonString = obj.toString();
    }

    @Before
    public void setup() throws Exception {
        tempDir = Files.createTempDir();
    }

    @After
    public void cleanup() {
        Files.deleteDir(tempDir);
    }

    @Test
    /**
     * Do basic put/get tests. Zookeeper is not tested here as it has additional constraints (e.g. keys begin with /)
     */
    public void runCorrectnessTest() throws Exception {
        String tableName = "testtable1";
        SpawnDataStore spawnDataStore;
        spawnDataStore = new PostgresqlDataStore("localhost", postgresPort, "template1", tableName, testProperties);
        correctnessTestDataStore(spawnDataStore);
        spawnDataStore.close();
        spawnDataStore = new H2DataStore(tempDir.getAbsolutePath(), tableName, testProperties);
        correctnessTestDataStore(spawnDataStore);
        spawnDataStore.close();
        spawnDataStore = new MysqlDataStore("localhost", mysqlPort, "test", tableName, testProperties);
        correctnessTestDataStore(spawnDataStore);
        spawnDataStore.close();
    }

    private void correctnessTestDataStore(SpawnDataStore spawnDataStore) throws Exception {
        String key1 = "key1";
        String val1 = "value1";
        String key2 = "key2";
        String val2 = "value!!\"{ !!'' }[,'],;';;'\n_\\";

        spawnDataStore.put(key1, "old");
        spawnDataStore.put(key1, val1);
        spawnDataStore.put(key2, val2);
        assertNull("should get null for non-inserted key", spawnDataStore.get("key5"));
        assertEquals("should get latest value", val1, spawnDataStore.get(key1));
        assertEquals("should correctly fetch value with extra characters", val2, spawnDataStore.get(key2));
        Map<String, String> expected = ImmutableMap.of(key1, val1, key2, val2);

        String nullKey = "nullkey";
        spawnDataStore.put(nullKey, "val");
        spawnDataStore.put(nullKey, null);
        assertNull("should get null for key inserted as null", spawnDataStore.get(nullKey));

        assertEquals("should get expected map from multi-fetch call", expected, spawnDataStore.get(new String[]{key1, key2, "otherKey", "other'Key\nwithWeird;;';Characters"}));
        spawnDataStore.putAsChild("parent", "child1", val1);
        spawnDataStore.putAsChild("parent", "child2", "val2");
        spawnDataStore.deleteChild("parent", "child2");
        spawnDataStore.putAsChild("parent", "child3", val2);
        spawnDataStore.put("parent", "parentvalue");
        List<String> expectedChildren = ImmutableList.of("child1", "child3");
        assertEquals("should get expected children list", expectedChildren, spawnDataStore.getChildrenNames("parent"));
        assertEquals("should get correct parent value", "parentvalue", spawnDataStore.get("parent"));
        Map<String, String> expectedChildrenMap = ImmutableMap.of("child1", val1, "child3", val2);
        assertEquals("should get expected children map", expectedChildrenMap, spawnDataStore.getAllChildren("parent"));
        assertEquals("should get empty list for non-existent parent", new ArrayList<String>(), spawnDataStore.getChildrenNames("PARENT_NO_EXIST"));
        assertEquals("should get empty map for non-existent parent", new HashMap<String, String>(), spawnDataStore.getAllChildren("PARENT_NO_EXIST"));
    }

    @Test
    /**
     * Test single-threaded read/write performance for each type of data store.
     * All values reported are in milliseconds, in the following order:
     * [small reads; small writes; small reads and writes; large reads; large writes; large reads and writes]
     * Startup and shutdown time is not included in the test.
     */
    public void perfTest() throws Exception {
        String tableName = "testtable2";
        for (int i = 0; i < 5; i++) {
            SpawnDataStore spawnDataStore;
            spawnDataStore = new ZookeeperDataStore(ZkUtil.makeStandardClient());
            performanceTestDataStore(spawnDataStore);
            spawnDataStore.close();
            spawnDataStore = new PostgresqlDataStore("localhost", postgresPort, "template1", tableName, testProperties);
            performanceTestDataStore(spawnDataStore);
            spawnDataStore.close();
            spawnDataStore = new H2DataStore(tempDir.getAbsolutePath(), tableName, testProperties);
            performanceTestDataStore(spawnDataStore);
            spawnDataStore.close();
            spawnDataStore = new MysqlDataStore("localhost", mysqlPort, "test", tableName, testProperties);
            performanceTestDataStore(spawnDataStore);
            spawnDataStore.close();
        }

    }

    private void performanceTestDataStore(SpawnDataStore spawnDataStore) throws Exception {
        long readSmallSum = 0;
        long writeSmallSum = 0;
        long readWriteSmallSum = 0;
        long readBigSum = 0;
        long writeBigSum = 0;
        long readWriteBigSum = 0;
        int tries = 10;
        int numReadWrites = 50;
        for (int i = 0; i < tries; i++) {
            writeSmallSum += writeTest(spawnDataStore, numReadWrites, false);
            readSmallSum += readTest(spawnDataStore, numReadWrites, false);
            readWriteSmallSum += readWriteTest(spawnDataStore, numReadWrites, false);
            writeBigSum += writeTest(spawnDataStore, numReadWrites, true);
            readBigSum += readTest(spawnDataStore, numReadWrites, true);
            readWriteBigSum += readWriteTest(spawnDataStore, numReadWrites, true);
        }
        System.out.println(spawnDataStore.getDescription() + Arrays.asList(readSmallSum, writeSmallSum, readWriteSmallSum, readBigSum, writeBigSum, readWriteBigSum));
    }

    private long readTest(SpawnDataStore spawnDataStore, int reads, boolean big) {
        long now = System.currentTimeMillis();
        for (int i = 0; i < reads; i++) {
            spawnDataStore.get("/" + Integer.toString(i) + (big ? "big" : ""));
        }
        return (System.currentTimeMillis() - now);
    }

    private long writeTest(SpawnDataStore spawnDataStore, int writes, boolean big) throws Exception {
        long now = System.currentTimeMillis();
        for (int i = 0; i < writes; i++) {
            spawnDataStore.put("/" + Integer.toString(i) + (big ? "big" : ""), (big ? bigJsonString : Integer.toHexString(i)));
        }
        return (System.currentTimeMillis() - now);

    }

    private long readWriteTest(SpawnDataStore spawnDataStore, int readWrites, boolean big) throws Exception {
        long now = System.currentTimeMillis();

        for (int i = 0; i < readWrites; i++) {
            spawnDataStore.get("/" + Integer.toString(i));
            spawnDataStore.put("/" + Integer.toString(i) + (big ? "big" : ""), (big ? bigJsonString : Integer.toHexString(i)));
        }
        return (System.currentTimeMillis() - now);

    }

    private static final int CONCURRENT_THREADS = 20;
    private static AtomicBoolean concurrentTestErrored;

    @Test
    /**
     * Test multithreaded correctness and performance. Do many writes in parallel, then verify that all writes happened successfully.
     */
    public void concurCorrectnessTest() throws Exception {
        String tableName = "testtable3";
        for (boolean big : Arrays.asList(false, true)) {
            for (int i = 0; i < 5; i++) {
                int readWrites = 80;
                SpawnDataStore spawnDataStore;
                spawnDataStore = new ZookeeperDataStore(ZkUtil.makeStandardClient());
                concurrentTest(spawnDataStore, readWrites, big);
                spawnDataStore.close();
                spawnDataStore = new PostgresqlDataStore("localhost", postgresPort, "template1", tableName, testProperties);
                concurrentTest(spawnDataStore, readWrites, big);
                spawnDataStore.close();
                spawnDataStore = new H2DataStore(tempDir.getAbsolutePath(), tableName, testProperties);
                concurrentTest(spawnDataStore, readWrites, big);
                spawnDataStore.close();
                spawnDataStore = new MysqlDataStore("localhost", mysqlPort, "test", tableName, testProperties);
                concurrentTest(spawnDataStore, readWrites, big);
                spawnDataStore.close();
            }
        }
    }

    private void concurrentTest(final SpawnDataStore spawnDataStore, int readWrites, final boolean big) throws Exception {
        concurrentTestErrored = new AtomicBoolean(false);
        ExecutorService executorService = MoreExecutors.getExitingExecutorService(new ScheduledThreadPoolExecutor(CONCURRENT_THREADS));
        for (int i=0; i<readWrites; i++) {
            executorService.submit(new ConcurrentTestWorker(spawnDataStore, i, big));
            executorService.submit(new ConcurrentTestWorker(spawnDataStore, readWrites - i, big));
        }
        long now = System.currentTimeMillis();
        executorService.shutdown();
        executorService.awaitTermination(30, TimeUnit.SECONDS);
        long rv = System.currentTimeMillis() - now;
        for (int i=0; i<readWrites; i++) {
            assertEquals( valueForConcurrentTest(i, big), spawnDataStore.get(keyForConcurrentTest(i, big)));
        }
        assertFalse("threads should not throw exceptions during concurrent test, type=" + spawnDataStore.getDescription(), concurrentTestErrored.get());
        System.out.println(spawnDataStore.getDescription() + ", " + (big ? "big" : "small") + " test: " + rv);
    }

    private class ConcurrentTestWorker implements Runnable {
        private final String key;
        private final String value;
        private final SpawnDataStore spawnDataStore;

        public ConcurrentTestWorker(SpawnDataStore spawnDataStore, int i, boolean big) {
            this.key = keyForConcurrentTest(i, big);
            this.value = valueForConcurrentTest(i, big);
            this.spawnDataStore = spawnDataStore;
        }
        @Override
        public void run() {
            try {
                spawnDataStore.put(key, value);
                spawnDataStore.get(key);
            } catch (Exception e) {
                System.out.println("EXECUTOR THREAD EXCEPTION: " + e);
                e.printStackTrace();
                concurrentTestErrored.compareAndSet(false, true);
            }
        }
    }

    private static String keyForConcurrentTest(int i, boolean big) {
        return "/" + Integer.toString(i) + (big ? "big" : "");
    }

    private static String valueForConcurrentTest(int i, boolean big) {
        return (big ? bigJsonString : Integer.toHexString(i));
    }


}
