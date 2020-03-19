// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.experoinc.janusgraph.diskstorage.foundationdb.isolation;

import static com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.ISOLATION_LEVEL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.experoinc.janusgraph.FoundationDBContainer;
import com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBStoreManager;
import com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBTx;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyValueStoreUtil;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * @author Florian Grieskamp (Florian.Grieskamp@gdata.de)
 */
@Testcontainers
public class FoundationDBSerializableTest extends FoundationDBIsolationTest {

    @Container public static final FoundationDBContainer fdbContainer = new FoundationDBContainer();

    @Override
    public OrderedKeyValueStoreManager openStorageManager() throws BackendException {
        return new FoundationDBStoreManager(
            fdbContainer.getFoundationDBConfiguration().set(ISOLATION_LEVEL, "serializable"));
    }

    @Test
    public void testIsolationLevel() throws BackendException {
        FoundationDBStoreManager fdbManager = (FoundationDBStoreManager) manager;
        assertEquals(fdbManager.getIsolationLevel(), FoundationDBTx.IsolationLevel.SERIALIZABLE);
    }

    @Test
    public void testSerializeTransactions() throws BackendException, InterruptedException {
        new Thread(() -> {
            try {
                StoreTransaction tx = manager.beginTransaction(getTxConfig());
                Thread.sleep(500);
                insert(0, "thread 1", tx);
                Thread.sleep(1000);
                assertEquals("thread 1", get(0, tx));
                tx.commit();
            } catch (Exception ignored) {}
        }).start();
        new Thread(() -> {
            try {
                StoreTransaction tx = manager.beginTransaction(getTxConfig());
                Thread.sleep(1000);
                insert(0, "thread 2", tx);
                Thread.sleep(1000);
                assertEquals("thread 2", get(0, tx));
                tx.commit();
            } catch (Exception ignored) {}
        }).start();
    }

    @Test
    public void readFailWithMaxRetries() throws BackendException {
        StoreTransaction tx = manager.beginTransaction(getTxConfig());
        doLongRunningRead(tx);
        assertThrows(BackendException.class, () -> tx.commit());
    }

    @Test
    public void writeFailWithMaxRetries() throws BackendException {
        StoreTransaction tx = manager.beginTransaction(getTxConfig());
        doLongRunningInsert(tx);
        assertThrows(BackendException.class, () -> tx.commit());
    }

    public void doLongRunningInsert(StoreTransaction tx) throws BackendException {
        long startTime = System.currentTimeMillis();
        int counter = 0;
        while (System.currentTimeMillis() < startTime + 6000) {
            insert(counter, String.valueOf(counter), tx);
            ++counter;
        }
    }

    public void doLongRunningRead(StoreTransaction tx) throws BackendException {
        long startTime = System.currentTimeMillis();
        int counter = 0;
        while (System.currentTimeMillis() < startTime + 6000) {
            contains(counter, tx);
            ++counter;
        }
    }

    public void insert(int key, String value, StoreTransaction tx) throws BackendException {
        store.insert(KeyValueStoreUtil.getBuffer(key), KeyValueStoreUtil.getBuffer(value), tx);
    }

    public String get(int key, StoreTransaction tx) throws BackendException {
        return KeyValueStoreUtil.getString(store.get(KeyValueStoreUtil.getBuffer(key), tx));
    }

    public boolean contains(int key, StoreTransaction tx) throws BackendException {
        return store.containsKey(KeyValueStoreUtil.getBuffer(key), tx);
    }
}
