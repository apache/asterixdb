/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.app.external;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.hyracks.api.io.FileSplit;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class ExternalLibraryJobUtilsTest {

    @Test
    public void oneSplitPerActiveNode() {
        IClusterStateManager csm = clusterOf(partition(0, "nc1", 0, true), partition(1, "nc1", 1, true),
                partition(2, "nc2", 0, true), partition(3, "nc2", 1, true));
        Assert.assertEquals(List.of("nc1", "nc2"), splitNodes(csm));
    }

    @Test
    public void inactivePartitionsAreSkipped() {
        // nc3 has failed: its partition is deactivated but still registered against it
        IClusterStateManager csm =
                clusterOf(partition(0, "nc1", 0, true), partition(1, "nc2", 0, true), partition(2, "nc3", 0, false));
        Assert.assertEquals(List.of("nc1", "nc2"), splitNodes(csm));
    }

    private static List<String> splitNodes(IClusterStateManager csm) {
        return Arrays.stream(ExternalLibraryJobUtils.getSplits(csm)).map(FileSplit::getNodeName)
                .collect(Collectors.toList());
    }

    private static ClusterPartition partition(int id, String node, int ioDevice, boolean active) {
        ClusterPartition partition = new ClusterPartition(id, node, ioDevice);
        partition.setActiveNodeId(node);
        partition.setActive(active);
        return partition;
    }

    private static IClusterStateManager clusterOf(ClusterPartition... partitions) {
        IClusterStateManager csm = Mockito.mock(IClusterStateManager.class);
        Mockito.when(csm.getClusterPartitons()).thenReturn(partitions);
        return csm;
    }
}
