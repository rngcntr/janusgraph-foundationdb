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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBStoreManager;
import com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBTx;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * @author Florian Grieskamp (Florian.Grieskamp@gdata.de)
 */
@Testcontainers
public class FoundationDBReadCommittedNoWriteTest extends FoundationDBIsolationTest {

    @Override
    protected String getIsolationLevel() {
        return "read_committed_no_write";
    }

    @Test
    @Override
    public void testIsolationLevel() throws BackendException {
        FoundationDBStoreManager fdbManager = (FoundationDBStoreManager) manager;
        assertEquals(FoundationDBTx.IsolationLevel.READ_COMMITTED_NO_WRITE,
                     fdbManager.getIsolationLevel());
    }

    @Test
    public void longReadSucceedWithoutException() throws BackendException {
        assertDoesNotThrow(() -> {
            StoreTransaction tx = manager.beginTransaction(getTxConfig());
            doLongRunningRead(tx);
            tx.commit();
        });
    }

    @Test
    public void longReadWriteSucceedWithoutException() throws BackendException {
        assertDoesNotThrow(() -> {
            StoreTransaction tx = manager.beginTransaction(getTxConfig());
            doLongRunningReadInsert(tx);
            tx.commit();
        });
    }

    @Test
    public void writePauseWriteSucceedWithRetry() throws BackendException {
        StoreTransaction tx = manager.beginTransaction(getTxConfig());
        assertDoesNotThrow(() -> {
            doWritePauseWrite(tx);
            tx.commit();
        });
    }

    @Test
    public void writeReadPauseWriteSucceedWithoutException() throws BackendException {
        StoreTransaction tx = manager.beginTransaction(getTxConfig());
        assertDoesNotThrow(() -> {
            doWriteReadPauseWrite(tx);
            tx.commit();
        });
    }
}
