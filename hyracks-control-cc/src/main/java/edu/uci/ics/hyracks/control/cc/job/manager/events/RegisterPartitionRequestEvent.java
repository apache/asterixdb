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

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.jobqueue.AbstractEvent;
import edu.uci.ics.hyracks.control.common.base.INodeController;
import edu.uci.ics.hyracks.control.common.job.PartitionState;

public class RegisterPartitionRequestEvent extends AbstractEvent {
    private final ClusterControllerService ccs;
    private final PartitionId pid;
    private final String nodeId;
    private final PartitionState minState;

    public RegisterPartitionRequestEvent(ClusterControllerService ccs, PartitionId pid, String nodeId,
            PartitionState minState) {
        this.ccs = ccs;
        this.pid = pid;
        this.nodeId = nodeId;
        this.minState = minState;
    }

    @Override
    public void run() {
        NodeControllerState ncs = ccs.getNodeMap().get(nodeId);
        if (ncs != null) {
            JobRun run = ccs.getRunMap().get(pid.getJobId());
            if (run == null) {
                return;
            }

            Map<PartitionId, Map<String, PartitionState>> partitionAvailabilityMap = run.getPartitionAvailabilityMap();
            Map<String, PartitionState> paMap = partitionAvailabilityMap.get(pid);
            boolean matched = false;
            if (paMap != null && !paMap.isEmpty()) {
                for (Map.Entry<String, PartitionState> pa : paMap.entrySet()) {
                    PartitionState state = pa.getValue();
                    if (state.isAtLeast(minState)) {
                        NodeControllerState availNcs = ccs.getNodeMap().get(pa.getKey());
                        if (availNcs != null) {
                            NetworkAddress networkAddress = availNcs.getDataPort();
                            try {
                                INodeController nc = ncs.getNodeController();
                                nc.reportPartitionAvailability(pid, networkAddress);
                                matched = true;
                                break;
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
            if (!matched) {
                Map<PartitionId, Map<String, PartitionState>> partitionRequestorMap = run.getPartitionRequestorMap();
                Map<String, PartitionState> prMap = partitionRequestorMap.get(pid);
                if (prMap == null) {
                    prMap = new HashMap<String, PartitionState>();
                    partitionRequestorMap.put(pid, prMap);
                }
                prMap.put(nodeId, minState);
            }
        }
    }

    @Override
    public String toString() {
        return "PartitionRequest@[" + nodeId + "][" + pid + "]" + minState;
    }
}