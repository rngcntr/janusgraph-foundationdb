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

import com.experoinc.janusgraph.FoundationDBContainer;
import com.google.common.collect.ImmutableMap;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
@Testcontainers
public class FoundationDBFixedLengthKCVSTest extends KeyColumnValueStoreTest {

    @Container public static final FoundationDBContainer fdbContainer = new FoundationDBContainer();

    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        FoundationDBStoreManager sm =
            new FoundationDBStoreManager(fdbContainer.getFoundationDBConfiguration());
        return new OrderedKeyValueStoreManagerAdapter(sm, ImmutableMap.of(storeName, 8));
    }

    @Test
    @Disabled
    @Override
    public void testConcurrentGetSlice() {
    }

    @Test
    @Disabled
    @Override
    public void testConcurrentGetSliceAndMutate() {
    }
}
