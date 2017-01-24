/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.api.job.resource;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.junit.Assert;
import org.junit.Test;

public class ClusterCapacityTest {

    @Test
    public void test() throws HyracksException {
        ClusterCapacity capacity = new ClusterCapacity();
        String nodeId = "node1";

        // Adds one node.
        capacity.update(nodeId, new NodeCapacity(1024L, 8));
        Assert.assertTrue(capacity.getAggregatedMemoryByteSize() == 1024L);
        Assert.assertTrue(capacity.getAggregatedCores() == 8);

        // Updates the node.
        capacity.update(nodeId, new NodeCapacity(-1L, -2));

        // Verifies that node is removed
        Assert.assertTrue(capacity.getAggregatedMemoryByteSize() == 0L);
        Assert.assertTrue(capacity.getAggregatedCores() == 0);

        boolean nodeNotExist = false;
        try {
            capacity.getMemoryByteSize(nodeId);
        } catch (HyracksException e) {
            nodeNotExist = e.getErrorCode() == ErrorCode.NO_SUCH_NODE;
        }
        Assert.assertTrue(nodeNotExist);
        nodeNotExist = false;
        try {
            capacity.getCores(nodeId);
        } catch (HyracksException e) {
            nodeNotExist = e.getErrorCode() == ErrorCode.NO_SUCH_NODE;
        }
        Assert.assertTrue(nodeNotExist);

        // Adds the node again.
        capacity.update(nodeId, new NodeCapacity(1024L, 8));
        // Updates the node.
        capacity.update(nodeId, new NodeCapacity(4L, 0));

        // Verifies that node does not exist
        Assert.assertTrue(capacity.getAggregatedMemoryByteSize() == 0L);
        Assert.assertTrue(capacity.getAggregatedCores() == 0);
        nodeNotExist = false;
        try {
            capacity.getMemoryByteSize(nodeId);
        } catch (HyracksException e) {
            nodeNotExist = e.getErrorCode() == ErrorCode.NO_SUCH_NODE;
        }
        Assert.assertTrue(nodeNotExist);
        nodeNotExist = false;
        try {
            capacity.getCores(nodeId);
        } catch (HyracksException e) {
            nodeNotExist = e.getErrorCode() == ErrorCode.NO_SUCH_NODE;
        }
        Assert.assertTrue(nodeNotExist);
    }

}
