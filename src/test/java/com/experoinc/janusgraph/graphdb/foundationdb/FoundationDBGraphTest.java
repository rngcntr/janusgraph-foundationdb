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

import static org.janusgraph.testutil.JanusGraphAssert.assertCount;
import static org.junit.Assert.*;

import com.experoinc.janusgraph.FoundationDBContainer;
import com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions;
import com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBTx.IsolationLevel;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
@Testcontainers
public class FoundationDBGraphTest extends JanusGraphTest {

    @Container
    public static final FoundationDBContainer fdbContainer = new FoundationDBContainer();

    private static final Logger log =
            LoggerFactory.getLogger(FoundationDBGraphTest.class);

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration modifiableConfiguration = fdbContainer.getFoundationDBConfiguration();
        String methodName = this.testInfo.getDisplayName();
        if (methodName.equals("testConsistencyEnforcement()") || methodName.equals("testConcurrentConsistencyEnforcement()")) {
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
    public void testConsistencyEnforcement() {
        // this test's isolation level is set to SERIALIZABLE in getConfiguration
        super.testConsistencyEnforcement();
    }

    @Test
    @Override
    public void testConcurrentConsistencyEnforcement() throws Exception {
        // this test's isolation level is set to SERIALIZABLE in getConfiguration
        super.testConcurrentConsistencyEnforcement();
    }

    @Test
    @Override
    public void testTinkerPopOptimizationStrategies() {
        super.testTinkerPopOptimizationStrategies();
    }

    @Test
    public void testIDBlockAllocationTimeout() throws BackendException {
        config.set("ids.authority.wait-time", Duration.of(0L, ChronoUnit.NANOS));
        config.set("ids.renew-timeout", Duration.of(1L, ChronoUnit.MILLIS));
        close();
        JanusGraphFactory.drop(graph);
        open(config);
        try {
            graph.addVertex();
            fail();
        } catch (JanusGraphException ignored) {

        }

        assertTrue(graph.isOpen());

        close(); // must be able to close cleanly

        // Must be able to reopen
        open(config);

        assertEquals(0L, (long) graph.traversal().V().count().next());
    }

    @Test
    @Override
    public void testLargeJointIndexRetrieval() {
        makeVertexIndexedKey("sid", Integer.class);
        makeVertexIndexedKey("color", String.class);
        finishSchema();

        int sids = 17;
        String[] colors = {"blue", "red", "yellow", "brown", "green", "orange", "purple"};
        int multiplier = 20; // reduce multiplier to not exceed transaction limit
        int numV = sids * colors.length * multiplier;
        for (int i = 0; i < numV; i++) {
            graph.addVertex("color", colors[i % colors.length], "sid", i % sids);
        }
        clopen();

        assertCount(numV / sids, graph.query().has("sid", 8).vertices());
        assertCount(numV / colors.length, graph.query().has("color", colors[2]).vertices());

        assertCount(multiplier, graph.query().has("sid", 11).has("color", colors[3]).vertices());
    }

    @Test
    @Override
    public void testVertexCentricQuery() {
        testVertexCentricQuery(100 /*noVertices*/);
    }
}
