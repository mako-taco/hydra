package com.addthis.hydra.job;

import com.addthis.hydra.job.store.JdbcDataStore;

import org.junit.Test;

public class JdbcDataStoreTest {

    @Test
    public void test() throws Exception {

        JdbcDataStore jdbcDataStore = new JdbcDataStore();
        jdbcDataStore.put("key6", "value6");
        System.out.println(jdbcDataStore.get("key5"));
        System.out.println(jdbcDataStore.get("key7"));
        // put, get, etc
        jdbcDataStore.close();
    }


}
