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
package edu.uci.ics.hyracks.control.cc.remote.ops;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import edu.uci.ics.hyracks.api.comm.Endpoint;
import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.PortInstanceId;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.control.cc.remote.RemoteOp;
import edu.uci.ics.hyracks.control.common.base.INodeController;

public class Phase2Installer implements RemoteOp<Void> {
    private String nodeId;
    private UUID jobId;
    private String appName;
    private JobPlan plan;
    private UUID stageId;
    private Map<ActivityNodeId, Set<Integer>> tasks;
    private Map<OperatorDescriptorId, Integer> opNumPartitions;
    private Map<PortInstanceId, Endpoint> globalPortMap;

    public Phase2Installer(String nodeId, UUID jobId, String appName, JobPlan plan, UUID stageId,
            Map<ActivityNodeId, Set<Integer>> tasks, Map<OperatorDescriptorId, Integer> opNumPartitions,
            Map<PortInstanceId, Endpoint> globalPortMap) {
        this.nodeId = nodeId;
        this.jobId = jobId;
        this.appName = appName;
        this.plan = plan;
        this.stageId = stageId;
        this.tasks = tasks;
        this.opNumPartitions = opNumPartitions;
        this.globalPortMap = globalPortMap;
    }

    @Override
    public Void execute(INodeController node) throws Exception {
        node.initializeJobletPhase2(appName, jobId, JavaSerializationUtils.serialize(plan), stageId, tasks,
                opNumPartitions, globalPortMap);
        return null;
    }

    @Override
    public String toString() {
        return jobId + " Distribution Phase 2";
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }
}