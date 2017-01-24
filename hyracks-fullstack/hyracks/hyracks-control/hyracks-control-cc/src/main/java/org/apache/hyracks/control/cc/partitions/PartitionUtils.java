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

package org.apache.hyracks.control.cc.partitions;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.common.base.INodeController;
import org.apache.hyracks.control.common.job.PartitionDescriptor;
import org.apache.hyracks.control.common.job.PartitionRequest;

public class PartitionUtils {
    public static void reportPartitionMatch(ClusterControllerService ccs, final PartitionId pid,
            Pair<PartitionDescriptor, PartitionRequest> match) throws Exception {
        PartitionDescriptor desc = match.getLeft();
        PartitionRequest req = match.getRight();

        INodeManager nodeManager = ccs.getNodeManager();
        NodeControllerState producerNCS = nodeManager.getNodeControllerState(desc.getNodeId());
        NodeControllerState requestorNCS = nodeManager.getNodeControllerState(req.getNodeId());
        final NetworkAddress dataport = producerNCS.getDataPort();
        final INodeController requestorNC = requestorNCS.getNodeController();
        requestorNC.reportPartitionAvailability(pid, dataport);
    }
}
