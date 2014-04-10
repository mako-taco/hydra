package com.addthis.hydra.job;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.addthis.basis.util.Files;

import com.addthis.bark.ZkUtil;
import com.addthis.hydra.job.store.H2DataStore;
import com.addthis.hydra.job.store.MysqlDataStore;
import com.addthis.hydra.job.store.PostgresqlDataStore;
import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.hydra.job.store.ZookeeperDataStore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JdbcDataStoreTest {

    File tempDir;

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
        jdbcDataStore = new PostgresqlDataStore("localhost", 5432, "template1", "testtable2");
        correctnessTestDataStore(jdbcDataStore);
        jdbcDataStore.close();
        jdbcDataStore = new H2DataStore(tempDir.getAbsolutePath(), "test");
        correctnessTestDataStore(jdbcDataStore);
        jdbcDataStore.close();
        jdbcDataStore = new MysqlDataStore("localhost", 3306, "test", "testtable");
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
        assertNull("should get null for key insterted as null", jdbcDataStore.get(nullKey));

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
            jdbcDataStore = new PostgresqlDataStore("localhost", 5432, "template1", "testtable2");
            performanceTestDataStore(jdbcDataStore);
            jdbcDataStore.close();
            jdbcDataStore = new H2DataStore(tempDir.getAbsolutePath(), "test");
            performanceTestDataStore(jdbcDataStore);
            jdbcDataStore.close();
            jdbcDataStore = new MysqlDataStore("localhost", 3306, "test", "testtable");
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
            jdbcDataStore.put("/" + Integer.toString(i) + (big ? "big" : ""), (big ? bigString : Integer.toHexString(i)));
        }
        return (System.currentTimeMillis() - now);

    }

    private long readWriteTest(SpawnDataStore jdbcDataStore, int readWrites, boolean big) throws Exception {
        long now = System.currentTimeMillis();

        for (int i = 0; i < readWrites; i++) {
            jdbcDataStore.get("/" + Integer.toString(i));
            jdbcDataStore.put("/" + Integer.toString(i) + (big ? "big" : ""), (big ? bigString : Integer.toHexString(i)));
        }
        return (System.currentTimeMillis() - now);

    }

    private final static String bigString;

    static {
        try {
            byte[] bytes = Files.read(new File("/Users/al/big.txt"));
            bigString = new String(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
