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

import com.experoinc.janusgraph.FoundationDBContainer;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphPartitionGraphTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
@Testcontainers
public class FoundationDBPartitionGraphTest extends JanusGraphPartitionGraphTest {

    final static int numPartitions = 8;

    @Container public static final FoundationDBContainer fdbContainer = new FoundationDBContainer();

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return fdbContainer.getFoundationDBGraphConfiguration();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration modifiableConfiguration =
            fdbContainer.getFoundationDBConfiguration();
        // Let GraphDatabaseConfiguration's config freezer set CLUSTER_PARTITION
        // modifiableConfiguration.set(GraphDatabaseConfiguration.CLUSTER_PARTITION,true);
        modifiableConfiguration.set(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS,
                                    numPartitions);
        // uses SimpleBulkPlacementStrategy by default
        modifiableConfiguration.set(SimpleBulkPlacementStrategy.CONCURRENT_PARTITIONS,
                                    3 * numPartitions);
        return modifiableConfiguration.getConfiguration();
    }
}
