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
package edu.uci.ics.hyracks.control.common.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.IActivityNode;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class JobPlan implements Serializable {
    private static final long serialVersionUID = 1L;

    private final JobSpecification jobSpec;

    private final EnumSet<JobFlag> jobFlags;

    private final Map<ActivityNodeId, IActivityNode> activityNodes;

    private final Map<ActivityNodeId, Set<ActivityNodeId>> blocker2blockedMap;

    private final Map<ActivityNodeId, Set<ActivityNodeId>> blocked2blockerMap;

    private final Map<OperatorDescriptorId, Set<ActivityNodeId>> operatorTaskMap;

    private final Map<ActivityNodeId, List<Integer>> taskInputMap;

    private final Map<ActivityNodeId, List<Integer>> taskOutputMap;

    private final Map<OperatorDescriptorId, List<ActivityNodeId>> operatorInputMap;

    private final Map<OperatorDescriptorId, List<ActivityNodeId>> operatorOutputMap;

    private JobStage endStage;

    public JobPlan(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) {
        this.jobSpec = jobSpec;
        this.jobFlags = jobFlags;
        activityNodes = new HashMap<ActivityNodeId, IActivityNode>();
        blocker2blockedMap = new HashMap<ActivityNodeId, Set<ActivityNodeId>>();
        blocked2blockerMap = new HashMap<ActivityNodeId, Set<ActivityNodeId>>();
        operatorTaskMap = new HashMap<OperatorDescriptorId, Set<ActivityNodeId>>();
        taskInputMap = new HashMap<ActivityNodeId, List<Integer>>();
        taskOutputMap = new HashMap<ActivityNodeId, List<Integer>>();
        operatorInputMap = new HashMap<OperatorDescriptorId, List<ActivityNodeId>>();
        operatorOutputMap = new HashMap<OperatorDescriptorId, List<ActivityNodeId>>();
    }

    public JobSpecification getJobSpecification() {
        return jobSpec;
    }

    public EnumSet<JobFlag> getJobFlags() {
        return jobFlags;
    }

    public Map<ActivityNodeId, IActivityNode> getActivityNodeMap() {
        return activityNodes;
    }

    public Map<ActivityNodeId, Set<ActivityNodeId>> getBlocker2BlockedMap() {
        return blocker2blockedMap;
    }

    public Map<ActivityNodeId, Set<ActivityNodeId>> getBlocked2BlockerMap() {
        return blocked2blockerMap;
    }

    public Map<OperatorDescriptorId, Set<ActivityNodeId>> getOperatorTaskMap() {
        return operatorTaskMap;
    }

    public Map<ActivityNodeId, List<Integer>> getTaskInputMap() {
        return taskInputMap;
    }

    public Map<ActivityNodeId, List<Integer>> getTaskOutputMap() {
        return taskOutputMap;
    }

    public Map<OperatorDescriptorId, List<ActivityNodeId>> getOperatorInputMap() {
        return operatorInputMap;
    }

    public Map<OperatorDescriptorId, List<ActivityNodeId>> getOperatorOutputMap() {
        return operatorOutputMap;
    }

    public void setEndStage(JobStage endStage) {
        this.endStage = endStage;
    }

    public JobStage getEndStage() {
        return endStage;
    }

    public List<IConnectorDescriptor> getTaskInputs(ActivityNodeId hanId) {
        List<Integer> inputIndexes = taskInputMap.get(hanId);
        if (inputIndexes == null) {
            return null;
        }
        OperatorDescriptorId ownerId = hanId.getOperatorDescriptorId();
        List<IConnectorDescriptor> inputs = new ArrayList<IConnectorDescriptor>();
        for (Integer i : inputIndexes) {
            inputs.add(jobSpec.getInputConnectorDescriptor(ownerId, i));
        }
        return inputs;
    }

    public List<IConnectorDescriptor> getTaskOutputs(ActivityNodeId hanId) {
        List<Integer> outputIndexes = taskOutputMap.get(hanId);
        if (outputIndexes == null) {
            return null;
        }
        OperatorDescriptorId ownerId = hanId.getOperatorDescriptorId();
        List<IConnectorDescriptor> outputs = new ArrayList<IConnectorDescriptor>();
        for (Integer i : outputIndexes) {
            outputs.add(jobSpec.getOutputConnectorDescriptor(ownerId, i));
        }
        return outputs;
    }

    public RecordDescriptor getTaskInputRecordDescriptor(ActivityNodeId hanId, int inputIndex) {
        int opInputIndex = getTaskInputMap().get(hanId).get(inputIndex);
        return jobSpec.getOperatorInputRecordDescriptor(hanId.getOperatorDescriptorId(), opInputIndex);
    }

    public RecordDescriptor getTaskOutputRecordDescriptor(ActivityNodeId hanId, int outputIndex) {
        int opOutputIndex = getTaskOutputMap().get(hanId).get(outputIndex);
        return jobSpec.getOperatorOutputRecordDescriptor(hanId.getOperatorDescriptorId(), opOutputIndex);
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("ActivityNodes: " + activityNodes);
        buffer.append('\n');
        buffer.append("Blocker->Blocked: " + blocker2blockedMap);
        buffer.append('\n');
        buffer.append("Blocked->Blocker: " + blocked2blockerMap);
        buffer.append('\n');
        return buffer.toString();
    }
}