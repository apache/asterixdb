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
package edu.uci.ics.hyracks.api.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IActivityNode;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.util.Pair;

public class JobActivityGraph implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String appName;

    private final JobSpecification jobSpec;

    private final EnumSet<JobFlag> jobFlags;

    private final Map<ActivityNodeId, IActivityNode> activityNodes;

    private final Map<ActivityNodeId, Set<ActivityNodeId>> blocker2blockedMap;

    private final Map<ActivityNodeId, Set<ActivityNodeId>> blocked2blockerMap;

    private final Map<OperatorDescriptorId, Set<ActivityNodeId>> operatorActivityMap;

    private final Map<ActivityNodeId, List<Integer>> activityInputMap;

    private final Map<ActivityNodeId, List<Integer>> activityOutputMap;

    private final Map<OperatorDescriptorId, List<ActivityNodeId>> operatorInputMap;

    private final Map<OperatorDescriptorId, List<ActivityNodeId>> operatorOutputMap;

    public JobActivityGraph(String appName, JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) {
        this.appName = appName;
        this.jobSpec = jobSpec;
        this.jobFlags = jobFlags;
        activityNodes = new HashMap<ActivityNodeId, IActivityNode>();
        blocker2blockedMap = new HashMap<ActivityNodeId, Set<ActivityNodeId>>();
        blocked2blockerMap = new HashMap<ActivityNodeId, Set<ActivityNodeId>>();
        operatorActivityMap = new HashMap<OperatorDescriptorId, Set<ActivityNodeId>>();
        activityInputMap = new HashMap<ActivityNodeId, List<Integer>>();
        activityOutputMap = new HashMap<ActivityNodeId, List<Integer>>();
        operatorInputMap = new HashMap<OperatorDescriptorId, List<ActivityNodeId>>();
        operatorOutputMap = new HashMap<OperatorDescriptorId, List<ActivityNodeId>>();
    }

    public String getApplicationName() {
        return appName;
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

    public Map<OperatorDescriptorId, Set<ActivityNodeId>> getOperatorActivityMap() {
        return operatorActivityMap;
    }

    public Map<ActivityNodeId, List<Integer>> getActivityInputMap() {
        return activityInputMap;
    }

    public Map<ActivityNodeId, List<Integer>> getActivityOutputMap() {
        return activityOutputMap;
    }

    public Map<OperatorDescriptorId, List<ActivityNodeId>> getOperatorInputMap() {
        return operatorInputMap;
    }

    public Map<OperatorDescriptorId, List<ActivityNodeId>> getOperatorOutputMap() {
        return operatorOutputMap;
    }

    public List<IConnectorDescriptor> getActivityInputConnectorDescriptors(ActivityNodeId hanId) {
        List<Integer> inputIndexes = activityInputMap.get(hanId);
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

    public List<IConnectorDescriptor> getActivityOutputConnectorDescriptors(ActivityNodeId hanId) {
        List<Integer> outputIndexes = activityOutputMap.get(hanId);
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

    public ActivityNodeId getConsumerActivity(ConnectorDescriptorId cdId) {
        Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>> connEdge = jobSpec
                .getConnectorOperatorMap().get(cdId);

        OperatorDescriptorId consumerOpId = connEdge.second.first.getOperatorId();
        int consumerInputIdx = connEdge.second.second;

        for (ActivityNodeId anId : operatorActivityMap.get(consumerOpId)) {
            List<Integer> anInputs = activityInputMap.get(anId);
            if (anInputs != null) {
                for (Integer idx : anInputs) {
                    if (idx.intValue() == consumerInputIdx) {
                        return anId;
                    }
                }
            }
        }
        return null;
    }

    public ActivityNodeId getProducerActivity(ConnectorDescriptorId cdId) {
        Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>> connEdge = jobSpec
                .getConnectorOperatorMap().get(cdId);

        OperatorDescriptorId producerOpId = connEdge.first.first.getOperatorId();
        int producerInputIdx = connEdge.first.second;

        for (ActivityNodeId anId : operatorActivityMap.get(producerOpId)) {
            List<Integer> anOutputs = activityOutputMap.get(anId);
            if (anOutputs != null) {
                for (Integer idx : anOutputs) {
                    if (idx.intValue() == producerInputIdx) {
                        return anId;
                    }
                }
            }
        }
        return null;
    }

    public RecordDescriptor getActivityInputRecordDescriptor(ActivityNodeId hanId, int inputIndex) {
        int opInputIndex = getActivityInputMap().get(hanId).get(inputIndex);
        return jobSpec.getOperatorInputRecordDescriptor(hanId.getOperatorDescriptorId(), opInputIndex);
    }

    public RecordDescriptor getActivityOutputRecordDescriptor(ActivityNodeId hanId, int outputIndex) {
        int opOutputIndex = getActivityOutputMap().get(hanId).get(outputIndex);
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