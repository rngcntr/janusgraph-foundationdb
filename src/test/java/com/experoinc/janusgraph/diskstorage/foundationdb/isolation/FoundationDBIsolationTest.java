package com.experoinc.janusgraph.diskstorage.foundationdb.isolation;

import static com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.ISOLATION_LEVEL;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.experoinc.janusgraph.FoundationDBContainer;
import com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBStoreManager;

import org.janusgraph.diskstorage.AbstractKCVSTest;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyValueStoreUtil;
import org.janusgraph.diskstorage.keycolumnvalue.StoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public abstract class FoundationDBIsolationTest extends AbstractKCVSTest {

    protected OrderedKeyValueStoreManager manager;
    protected OrderedKeyValueStore store;

    @Container
    protected static final FoundationDBContainer fdbContainer = new FoundationDBContainer();

    public OrderedKeyValueStoreManager openStorageManager() throws BackendException {
        return new FoundationDBStoreManager(
            fdbContainer.getFoundationDBConfiguration().set(ISOLATION_LEVEL, getIsolationLevel()));
    }

    protected abstract String getIsolationLevel();

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

    public abstract void testIsolationLevel() throws BackendException;

    @Test
    public void testIsolateTransactions() throws BackendException, InterruptedException {
        Thread t1 = new Thread(() -> {
            try {
                StoreTransaction tx = manager.beginTransaction(getTxConfig());
                Thread.sleep(500);
                insert(0, "thread 1 old", tx);
                Thread.sleep(1000);
                assertEquals("thread 1 old", get(0, tx));
                Thread.sleep(1000);
                insert(0, "thread 1 new", tx);
                Thread.sleep(1000);
                assertEquals("thread 1 new", get(0, tx));
                tx.commit();
            } catch (Exception ignored) {
            }
        });
        Thread t2 = new Thread(() -> {
            try {
                StoreTransaction tx = manager.beginTransaction(getTxConfig());
                Thread.sleep(1000);
                insert(0, "thread 2 old", tx);
                Thread.sleep(1000);
                assertEquals("thread 2 old", get(0, tx));
                Thread.sleep(1000);
                insert(0, "thread 2 new", tx);
                Thread.sleep(1000);
                assertEquals("thread 2 new", get(0, tx));
                tx.commit();
            } catch (Exception ignored) {
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();
    }

    public void close() throws BackendException {
        store.close();
        manager.close();
    }

    protected void insert(int key, String value, StoreTransaction tx) throws BackendException {
        store.insert(KeyValueStoreUtil.getBuffer(key), KeyValueStoreUtil.getBuffer(value), tx);
    }

    protected String get(int key, StoreTransaction tx) throws BackendException {
        return KeyValueStoreUtil.getString(store.get(KeyValueStoreUtil.getBuffer(key), tx));
    }

    /**
     * This method is intentionally inefficient.
     */
    protected String getFirstOfRange(int start, int end, StoreTransaction tx)
        throws BackendException {
        KVQuery query =
            new KVQuery(KeyValueStoreUtil.getBuffer(start), KeyValueStoreUtil.getBuffer(end));
        return KeyValueStoreUtil.getString(store.getSlice(query, tx).next().getValue());
    }

    protected boolean contains(int key, StoreTransaction tx) throws BackendException {
        return store.containsKey(KeyValueStoreUtil.getBuffer(key), tx);
    }

    protected void doLongRunningRead(StoreTransaction tx) throws BackendException {
        long startTime = System.currentTimeMillis();
        int counter = 0;
        while (System.currentTimeMillis() < startTime + 10000) {
            contains(counter, tx);
            ++counter;
        }
    }

    protected void doLongRunningReadInsert(StoreTransaction tx) throws BackendException {
        long startTime = System.currentTimeMillis();
        int counter = 0;
        // TODO: Find out why 6000ms is not enough here to force a restart of the transaction
        while (System.currentTimeMillis() < startTime + 10_000) {
            insert(counter, String.valueOf(1), tx);
            counter += Integer.parseInt(getFirstOfRange(0, counter + 1, tx));
        }
    }

    protected void doWritePauseWrite(StoreTransaction tx) throws Exception {
        insert(0, "0", tx);
        Thread.sleep(10_000);
        insert(1, "1", tx);
    }

    protected void doWriteReadPauseWrite(StoreTransaction tx) throws Exception {
        insert(0, "0", tx);
        if(!contains(1, tx)) {
            Thread.sleep(10_000);
        }
        insert(1, "1", tx);
    }
}
