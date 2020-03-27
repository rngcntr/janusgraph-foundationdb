// Copyright 2018 Expero Inc.
//
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

package com.experoinc.janusgraph.diskstorage.foundationdb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.experoinc.janusgraph.FoundationDBContainer;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyValueStoreTest;
import org.janusgraph.diskstorage.KeyValueStoreUtil;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
@Testcontainers
public class FoundationDBKeyValueTest extends KeyValueStoreTest {

    @Container public static final FoundationDBContainer fdbContainer = new FoundationDBContainer();

    @Override
    public OrderedKeyValueStoreManager openStorageManager() throws BackendException {
        return new FoundationDBStoreManager(fdbContainer.getFoundationDBConfiguration());
    }

    @Test
    public void readYourWritesTest() throws BackendException {
        store.insert(KeyValueStoreUtil.getBuffer(0), KeyValueStoreUtil.getBuffer("test"), tx, null);
        StaticBuffer output = store.get(KeyValueStoreUtil.getBuffer(0), tx);
        assertEquals(0, KeyValueStoreUtil.getBuffer("test").compareTo(output));
    }
}
