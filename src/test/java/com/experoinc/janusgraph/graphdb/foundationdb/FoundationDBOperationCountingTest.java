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

package com.experoinc.janusgraph.graphdb.foundationdb;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

import com.experoinc.janusgraph.FoundationDBContainer;
import com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions;
import com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBTx.IsolationLevel;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphOperationCountingTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
@Testcontainers
public class FoundationDBOperationCountingTest extends JanusGraphOperationCountingTest {

    @Container
    public static final FoundationDBContainer fdbContainer = new FoundationDBContainer();

    private static final Logger log =
            LoggerFactory.getLogger(FoundationDBOperationCountingTest.class);

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return fdbContainer.getFoundationDBGraphConfiguration();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration modifiableConfiguration = fdbContainer.getFoundationDBConfiguration();
        modifiableConfiguration.set(BASIC_METRICS,true);
        modifiableConfiguration.set(METRICS_MERGE_STORES,false);
        modifiableConfiguration.set(PROPERTY_PREFETCHING,false);
        modifiableConfiguration.set(DB_CACHE, false);

        String methodName = this.testInfo.getDisplayName();
        if (methodName.equals("testConcurrentConsistencyEnforcement()")) {
            IsolationLevel iso = IsolationLevel.SERIALIZABLE;
            log.debug("Forcing isolation level {} for test method {}", iso, methodName);
            modifiableConfiguration.set(FoundationDBConfigOptions.ISOLATION_LEVEL, iso.toString());
        } else {
            IsolationLevel iso = null;
            if (modifiableConfiguration.has(FoundationDBConfigOptions.ISOLATION_LEVEL)) {
                iso = ConfigOption.getEnumValue(modifiableConfiguration.get(FoundationDBConfigOptions.ISOLATION_LEVEL),IsolationLevel.class);
            }
            log.debug("Using isolation level {} (null means adapter default) for test method {}", iso, methodName);
        }

        return modifiableConfiguration.getConfiguration();
    }

    @Test
    @Override
    public void testCacheConcurrency() throws InterruptedException {
        // this test fails sometimes for unknown reasons
        super.testCacheConcurrency();
    }
}
