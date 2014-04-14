package com.addthis.hydra.job;

import java.io.File;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JdbcDataStoreTest {

    File tempDir;
    /**
     * A big string to use as a test value. Constructed as a JSON map in order to simulate the things Spawn typically stores.
     */
    private final static String bigJsonString;

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
    public void runCorrectnessTest() throws Exception {
        SpawnDataStore jdbcDataStore;
        jdbcDataStore = new PostgresqlDataStore("localhost", 5432, "template1", "testtable29");
        correctnessTestDataStore(jdbcDataStore);
        jdbcDataStore.close();
        jdbcDataStore = new H2DataStore(tempDir.getAbsolutePath(), "test2");
        correctnessTestDataStore(jdbcDataStore);
        jdbcDataStore.close();
        jdbcDataStore = new MysqlDataStore("localhost", 3306, "test", "testtable2");
        correctnessTestDataStore(jdbcDataStore);
        jdbcDataStore.close();
    }

    public void correctnessTestDataStore(SpawnDataStore jdbcDataStore) throws Exception {
        String key1 = "key1";
        String val1 = "value1";
        String key2 = "key2";
        String val2 = "value!!\"{ !!'' }[,'],;';;'\n_\\";

        jdbcDataStore.put(key1, "old");
        jdbcDataStore.put(key1, val1);
        jdbcDataStore.put(key2, val2);
        assertNull("should get null for non-inserted key", jdbcDataStore.get("key5"));
        assertEquals("should get latest value", val1, jdbcDataStore.get(key1));
        assertEquals("should correctly fetch value with extra characters", val2, jdbcDataStore.get(key2));
        Map<String, String> expected = ImmutableMap.of(key1, val1, key2, val2);

        String nullKey = "nullkey";
        jdbcDataStore.put(nullKey, "val");
        jdbcDataStore.put(nullKey, null);
        assertNull("should get null for key inserted as null", jdbcDataStore.get(nullKey));

        assertEquals("should get expected map from multi-fetch call", expected, jdbcDataStore.get(new String[]{key1, key2, "otherKey", "other'Key\nwithWeird;;';Characters"}));
        jdbcDataStore.putAsChild("parent", "child1", val1);
        jdbcDataStore.putAsChild("parent", "child2", "val2");
        jdbcDataStore.deleteChild("parent", "child2");
        jdbcDataStore.putAsChild("parent", "child3", val2);
        List<String> expectedChildren = ImmutableList.of("child1", "child3");
        assertEquals("should get expected children list", expectedChildren, jdbcDataStore.getChildrenNames("parent"));
        Map<String, String> expectedChildrenMap = ImmutableMap.of("child1", val1, "child3", val2);
        assertEquals("should get expected children map", expectedChildrenMap, jdbcDataStore.getAllChildren("parent"));
        assertEquals("should get empty list for non-existent parent", new ArrayList<String>(), jdbcDataStore.getChildrenNames("PARENT_NO_EXIST"));
        assertEquals("should get empty map for non-existent parent", new HashMap<String, String>(), jdbcDataStore.getAllChildren("PARENT_NO_EXIST"));
    }

    @Test
    public void perfTest() throws Exception {
        for (int i = 0; i < 5; i++) {
            SpawnDataStore jdbcDataStore;
            jdbcDataStore = new ZookeeperDataStore(ZkUtil.makeStandardClient());
            performanceTestDataStore(jdbcDataStore);
            jdbcDataStore.close();
            jdbcDataStore = new PostgresqlDataStore("localhost", 5432, "template1", "testtable10");
            performanceTestDataStore(jdbcDataStore);
            jdbcDataStore.close();
            jdbcDataStore = new H2DataStore(tempDir.getAbsolutePath(), "test");
            performanceTestDataStore(jdbcDataStore);
            jdbcDataStore.close();
            jdbcDataStore = new MysqlDataStore("localhost", 3306, "test", "testtable9");
            performanceTestDataStore(jdbcDataStore);
            jdbcDataStore.close();
        }

    }

    public void performanceTestDataStore(SpawnDataStore spawnDataStore) throws Exception {
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

    private long readTest(SpawnDataStore jdbcDataStore, int reads, boolean big) {
        long now = System.currentTimeMillis();
        for (int i = 0; i < reads; i++) {
            jdbcDataStore.get("/" + Integer.toString(i) + (big ? "big" : ""));
        }
        return (System.currentTimeMillis() - now);
    }

    private long writeTest(SpawnDataStore jdbcDataStore, int writes, boolean big) throws Exception {
        long now = System.currentTimeMillis();
        for (int i = 0; i < writes; i++) {
            jdbcDataStore.put("/" + Integer.toString(i) + (big ? "big" : ""), (big ? bigJsonString : Integer.toHexString(i)));
        }
        return (System.currentTimeMillis() - now);

    }

    private long readWriteTest(SpawnDataStore jdbcDataStore, int readWrites, boolean big) throws Exception {
        long now = System.currentTimeMillis();

        for (int i = 0; i < readWrites; i++) {
            jdbcDataStore.get("/" + Integer.toString(i));
            jdbcDataStore.put("/" + Integer.toString(i) + (big ? "big" : ""), (big ? bigJsonString : Integer.toHexString(i)));
        }
        return (System.currentTimeMillis() - now);

    }

    private static final int CONCURRENT_THREADS = 20;

    @Test
    public void concurCorrectnessTest() throws Exception {
        for (int i = 0; i < 5; i++) {
            int readWrites = 80;
            SpawnDataStore jdbcDataStore;
            jdbcDataStore = new ZookeeperDataStore(ZkUtil.makeStandardClient());
            concurrentTest(jdbcDataStore, readWrites, false);
            jdbcDataStore.close();
            jdbcDataStore = new PostgresqlDataStore("localhost", 5432, "template1", "CCtesttable2");
            concurrentTest(jdbcDataStore, readWrites, false);
            jdbcDataStore.close();
            jdbcDataStore = new H2DataStore(tempDir.getAbsolutePath(), "CCtest3");
            concurrentTest(jdbcDataStore, readWrites, false);
            jdbcDataStore.close();
            jdbcDataStore = new MysqlDataStore("localhost", 3306, "test", "CCtesttable2");
            concurrentTest(jdbcDataStore, readWrites, false);
            jdbcDataStore.close();
        }
    }



    private void concurrentTest(final SpawnDataStore jdbcDataStore, int readWrites, final boolean big) throws Exception {
        try {
            ExecutorService executorService = MoreExecutors.getExitingExecutorService(new ScheduledThreadPoolExecutor(CONCURRENT_THREADS));
            for (int i=0; i<readWrites; i++) {
                final int j = i;
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        String key = "/" + Integer.toString(j) + (big ? "big" : "");
                        try {
                            jdbcDataStore.put(key, (big ? bigJsonString : Integer.toHexString(j)));
                            jdbcDataStore.get(key);
                        } catch (Exception e) {
                            System.out.println("EXECUTOR THREAD EXCEPTION: " + e);
                        }
                    }
                });
            }
            long now = System.currentTimeMillis();
            executorService.shutdown();
            executorService.awaitTermination(30, TimeUnit.SECONDS);
            long rv = System.currentTimeMillis() - now;
            for (int i=0; i<readWrites; i++) {
                assertEquals( (big ? bigJsonString : Integer.toHexString(i)), jdbcDataStore.get("/" + Integer.toString(i) + (big ? "big" : "")));
            }
            System.out.println(jdbcDataStore.getDescription() + ": " + rv);
        } catch (Exception ex) {
            System.out.println(jdbcDataStore.getDescription() + " FAILED");
        }


    }


}
