package com.addthis.hydra.job;

import java.util.List;
import java.util.Map;

import com.addthis.hydra.job.store.JdbcDataStore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JdbcDataStoreTest {

    @Test
    public void test() throws Exception {

        JdbcDataStore jdbcDataStore = new JdbcDataStore("SpawnData1");
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
        assertEquals("should get expected map from multi-fetch call", expected, jdbcDataStore.get(new String[]{key1, key2, "otherKey"}));
        jdbcDataStore.putAsChild("parent", "child1", val1);
        jdbcDataStore.putAsChild("parent", "child2", "val2");
        jdbcDataStore.deleteChild("parent", "child2");
        jdbcDataStore.putAsChild("parent", "child3", val2);
        List<String> expectedChildren = ImmutableList.of("child1", "child3");
        assertEquals("should get expected children list", expectedChildren, jdbcDataStore.getChildrenNames("parent"));
        Map<String, String> expectedChildrenMap = ImmutableMap.of("child1", val1, "child3", val2);
        assertEquals("should get expected children map", expectedChildrenMap, jdbcDataStore.getAllChildren("parent"));
        jdbcDataStore.close();
    }


}
