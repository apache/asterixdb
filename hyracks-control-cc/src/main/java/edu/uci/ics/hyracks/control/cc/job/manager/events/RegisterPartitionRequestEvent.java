/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.control.cc.job.manager.events;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.jobqueue.AbstractEvent;
import edu.uci.ics.hyracks.control.common.base.INodeController;

public class RegisterPartitionRequestEvent extends AbstractEvent {
    private final ClusterControllerService ccs;
    private final Collection<PartitionId> requiredPartitionIds;
    private final String nodeId;

    public RegisterPartitionRequestEvent(ClusterControllerService ccs, Collection<PartitionId> requiredPartitionIds,
            String nodeId) {
        this.ccs = ccs;
        this.requiredPartitionIds = requiredPartitionIds;
        this.nodeId = nodeId;
    }

    @Override
    public void run() {
        NodeControllerState ncs = ccs.getNodeMap().get(nodeId);
        if (ncs != null) {
            for (PartitionId pid : requiredPartitionIds) {
                JobRun run = ccs.getRunMap().get(pid.getJobId());
                if (run == null) {
                    return;
                }

                Map<PartitionId, Set<String>> partitionAvailabilityMap = run.getPartitionAvailabilityMap();
                Set<String> paSet = partitionAvailabilityMap.get(pid);
                boolean matched = false;
                if (paSet != null && !paSet.isEmpty()) {
                    for (String availablePartitionLocation : paSet) {
                        NodeControllerState availNcs = ccs.getNodeMap().get(availablePartitionLocation);
                        if (availNcs != null) {
                            NetworkAddress networkAddress = availNcs.getDataPort();
                            try {
                                INodeController nc = ncs.getNodeController();
                                nc.reportPartitionAvailability(pid, networkAddress);
                                matched = true;
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
                if (!matched) {
                    Map<PartitionId, Set<String>> partitionRequestorMap = run.getPartitionRequestorMap();
                    Set<String> prSet = partitionRequestorMap.get(pid);
                    if (prSet == null) {
                        prSet = new HashSet<String>();
                        partitionRequestorMap.put(pid, prSet);
                    }
                    prSet.add(nodeId);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "PartitionRequest@[" + nodeId + "][" + requiredPartitionIds + "]";
    }
}