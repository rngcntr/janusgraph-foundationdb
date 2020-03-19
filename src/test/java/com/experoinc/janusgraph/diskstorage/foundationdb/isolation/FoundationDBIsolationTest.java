package com.experoinc.janusgraph.diskstorage.foundationdb.isolation;

import org.janusgraph.diskstorage.AbstractKCVSTest;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.keycolumnvalue.StoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class FoundationDBIsolationTest extends AbstractKCVSTest {

    protected OrderedKeyValueStoreManager manager;
    protected OrderedKeyValueStore store;

    @BeforeEach
    public void setUp() throws Exception {
        StoreManager m = openStorageManager();
        m.clearStorage();
        m.close();
        open();
    }

    public void open() throws BackendException {
        manager = openStorageManager();
        String storeName = "testStore1";
        store = manager.openDatabase(storeName);
    }

    @AfterEach
    public void tearDown() throws Exception {
        close();
    }

    public void close() throws BackendException {
        store.close();
        manager.close();
    }

    public abstract OrderedKeyValueStoreManager openStorageManager() throws BackendException;
}
