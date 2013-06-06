/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.control.cc.partitions;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.common.base.INodeController;
import edu.uci.ics.hyracks.control.common.job.PartitionDescriptor;
import edu.uci.ics.hyracks.control.common.job.PartitionRequest;

public class PartitionUtils {
    public static void reportPartitionMatch(ClusterControllerService ccs, final PartitionId pid,
            Pair<PartitionDescriptor, PartitionRequest> match) throws Exception {
        PartitionDescriptor desc = match.getLeft();
        PartitionRequest req = match.getRight();

        NodeControllerState producerNCS = ccs.getNodeMap().get(desc.getNodeId());
        NodeControllerState requestorNCS = ccs.getNodeMap().get(req.getNodeId());
        final NetworkAddress dataport = producerNCS.getDataPort();
        final INodeController requestorNC = requestorNCS.getNodeController();
        requestorNC.reportPartitionAvailability(pid, dataport);
    }
}