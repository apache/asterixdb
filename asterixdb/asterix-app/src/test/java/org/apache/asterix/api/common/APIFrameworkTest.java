/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.asterix.api.common;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.client.IClusterInfoCollector;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.junit.Assert;
import org.junit.Test;

import junit.extensions.PA;

public class APIFrameworkTest {

    @Test
    public void testGetComputationLocations() throws Exception {
        IClusterInfoCollector clusterInfoCollector = mock(IClusterInfoCollector.class);

        // Constructs mocked cluster nodes.
        Map<String, NodeControllerInfo> map = new HashMap<>();
        NodeControllerInfo nc1Info = mock(NodeControllerInfo.class);
        when(nc1Info.getNumCores()).thenReturn(4);
        NodeControllerInfo nc2Info = mock(NodeControllerInfo.class);
        when(nc2Info.getNumCores()).thenReturn(4);
        String nc1 = "nc1";
        String nc2 = "nc2";
        map.put(nc1, nc1Info);
        map.put(nc2, nc2Info);
        when(clusterInfoCollector.getNodeControllerInfos()).thenReturn(map);

        // Creates an APIFramework.
        APIFramework apiFramework = new APIFramework(mock(ILangCompilationProvider.class));

        // Tests odd number parallelism.
        AlgebricksAbsolutePartitionConstraint loc = (AlgebricksAbsolutePartitionConstraint) PA.invokeMethod(
                apiFramework, "getComputationLocations(" + IClusterInfoCollector.class.getName() + ",int)",
                clusterInfoCollector, 5);
        int nc1Count = 0, nc2Count = 0;
        String[] partitions = loc.getLocations();
        for (String partition : partitions) {
            if (partition.equals(nc1)) {
                ++nc1Count;
            }
            if (partition.equals(nc2)) {
                ++nc2Count;
            }
        }
        Assert.assertTrue(nc1Count > 0);
        Assert.assertTrue(nc2Count > 0);
        Assert.assertTrue(Math.abs(nc1Count - nc2Count) == 1); // Tests load balance.
        Assert.assertTrue(partitions.length == 5);

        // Tests even number parallelism.
        loc = (AlgebricksAbsolutePartitionConstraint) PA.invokeMethod(apiFramework,
                "getComputationLocations(" + IClusterInfoCollector.class.getName() + ",int)", clusterInfoCollector, 8);
        nc1Count = 0;
        nc2Count = 0;
        partitions = loc.getLocations();
        for (String partition : partitions) {
            if (partition.equals(nc1)) {
                ++nc1Count;
            }
            if (partition.equals(nc2)) {
                ++nc2Count;
            }
        }
        Assert.assertTrue(nc1Count > 0);
        Assert.assertTrue(nc2Count > 0);
        Assert.assertTrue(Math.abs(nc1Count - nc2Count) == 0); // Tests load balance.
        // The maximum parallelism cannot be beyond n *(#core-1), where n is the number of NCs and #core is the number
        // of cores per NC.
        Assert.assertTrue(partitions.length == 6);

        // Tests the case when parallelism is one.
        loc = (AlgebricksAbsolutePartitionConstraint) PA.invokeMethod(apiFramework,
                "getComputationLocations(" + IClusterInfoCollector.class.getName() + ",int)", clusterInfoCollector, 1);
        Assert.assertTrue(loc.getLocations().length == 1);

        // Tests the case when parallelism is a negative.
        // In this case, the compiler has no idea and falls back to the default setting where all possible cores
        // are used.
        loc = (AlgebricksAbsolutePartitionConstraint) PA.invokeMethod(apiFramework,
                "getComputationLocations(" + IClusterInfoCollector.class.getName() + ",int)", clusterInfoCollector,
                -100);
        Assert.assertTrue(loc.getLocations().length == 6);

        // Tests the case when parallelism is -1.
        // In this case, the compiler has no idea and falls back to the default setting where all possible cores
        // are used.
        loc = (AlgebricksAbsolutePartitionConstraint) PA.invokeMethod(apiFramework,
                "getComputationLocations(" + IClusterInfoCollector.class.getName() + ",int)", clusterInfoCollector, -1);
        Assert.assertTrue(loc.getLocations().length == 6);

        // Tests the case when parallelism is zero.
        // In this case, the compiler has no idea and falls back to the default setting where all possible cores
        // are used.
        loc = (AlgebricksAbsolutePartitionConstraint) PA.invokeMethod(apiFramework,
                "getComputationLocations(" + IClusterInfoCollector.class.getName() + ",int)", clusterInfoCollector, 0);
        Assert.assertTrue(loc.getLocations().length == 6);

        // Verifies the number of calls on clusterInfoCollector.getNodeControllerInfos() in
        // APIFramework.getComputationLocations(...).
        verify(clusterInfoCollector, times(6)).getNodeControllerInfos();
    }

}
