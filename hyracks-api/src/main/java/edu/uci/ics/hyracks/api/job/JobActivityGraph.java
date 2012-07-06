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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicyAssignmentPolicy;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class JobActivityGraph implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String appName;

    private final EnumSet<JobFlag> jobFlags;

    private final Map<ActivityId, IActivity> activityMap;

    private final Map<ConnectorDescriptorId, IConnectorDescriptor> connectorMap;

    private final Map<ConnectorDescriptorId, RecordDescriptor> connectorRecordDescriptorMap;

    private final Map<ActivityId, List<IConnectorDescriptor>> activityInputMap;

    private final Map<ActivityId, List<IConnectorDescriptor>> activityOutputMap;

    private final Map<ConnectorDescriptorId, Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>>> connectorActivityMap;

    private final Map<ActivityId, Set<ActivityId>> blocker2blockedMap;

    private final Map<ActivityId, Set<ActivityId>> blocked2blockerMap;

    private IConnectorPolicyAssignmentPolicy connectorPolicyAssignmentPolicy;

    private int maxReattempts;

    private IJobletEventListenerFactory jobletEventListenerFactory;

    private IGlobalJobDataFactory globalJobDataFactory;

    public JobActivityGraph(String appName, EnumSet<JobFlag> jobFlags) {
        this.appName = appName;
        this.jobFlags = jobFlags;
        activityMap = new HashMap<ActivityId, IActivity>();
        connectorMap = new HashMap<ConnectorDescriptorId, IConnectorDescriptor>();
        connectorRecordDescriptorMap = new HashMap<ConnectorDescriptorId, RecordDescriptor>();
        activityInputMap = new HashMap<ActivityId, List<IConnectorDescriptor>>();
        activityOutputMap = new HashMap<ActivityId, List<IConnectorDescriptor>>();
        connectorActivityMap = new HashMap<ConnectorDescriptorId, Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>>>();
        blocker2blockedMap = new HashMap<ActivityId, Set<ActivityId>>();
        blocked2blockerMap = new HashMap<ActivityId, Set<ActivityId>>();
    }

    public String getApplicationName() {
        return appName;
    }

    public EnumSet<JobFlag> getJobFlags() {
        return jobFlags;
    }

    public Map<ActivityId, IActivity> getActivityMap() {
        return activityMap;
    }

    public Map<ConnectorDescriptorId, IConnectorDescriptor> getConnectorMap() {
        return connectorMap;
    }

    public Map<ConnectorDescriptorId, RecordDescriptor> getConnectorRecordDescriptorMap() {
        return connectorRecordDescriptorMap;
    }

    public Map<ActivityId, Set<ActivityId>> getBlocker2BlockedMap() {
        return blocker2blockedMap;
    }

    public Map<ActivityId, Set<ActivityId>> getBlocked2BlockerMap() {
        return blocked2blockerMap;
    }

    public Map<ActivityId, List<IConnectorDescriptor>> getActivityInputMap() {
        return activityInputMap;
    }

    public Map<ActivityId, List<IConnectorDescriptor>> getActivityOutputMap() {
        return activityOutputMap;
    }

    public Map<ConnectorDescriptorId, Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>>> getConnectorActivityMap() {
        return connectorActivityMap;
    }

    public ActivityId getConsumerActivity(ConnectorDescriptorId cdId) {
        Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>> connEdge = connectorActivityMap.get(cdId);
        return connEdge.getRight().getLeft().getActivityId();
    }

    public ActivityId getProducerActivity(ConnectorDescriptorId cdId) {
        Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>> connEdge = connectorActivityMap.get(cdId);
        return connEdge.getLeft().getLeft().getActivityId();
    }

    public IConnectorPolicyAssignmentPolicy getConnectorPolicyAssignmentPolicy() {
        return connectorPolicyAssignmentPolicy;
    }

    public void setConnectorPolicyAssignmentPolicy(IConnectorPolicyAssignmentPolicy connectorPolicyAssignmentPolicy) {
        this.connectorPolicyAssignmentPolicy = connectorPolicyAssignmentPolicy;
    }

    public void setMaxReattempts(int maxReattempts) {
        this.maxReattempts = maxReattempts;
    }

    public int getMaxReattempts() {
        return maxReattempts;
    }

    public IJobletEventListenerFactory getJobletEventListenerFactory() {
        return jobletEventListenerFactory;
    }

    public void setJobletEventListenerFactory(IJobletEventListenerFactory jobletEventListenerFactory) {
        this.jobletEventListenerFactory = jobletEventListenerFactory;
    }

    public IGlobalJobDataFactory getGlobalJobDataFactory() {
        return globalJobDataFactory;
    }

    public void setGlobalJobDataFactory(IGlobalJobDataFactory globalJobDataFactory) {
        this.globalJobDataFactory = globalJobDataFactory;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("ActivityNodes: " + activityMap);
        buffer.append('\n');
        buffer.append("Blocker->Blocked: " + blocker2blockedMap);
        buffer.append('\n');
        buffer.append("Blocked->Blocker: " + blocked2blockerMap);
        buffer.append('\n');
        return buffer.toString();
    }

    public JSONObject toJSON() throws JSONException {
        JSONObject jplan = new JSONObject();

        jplan.put("flags", jobFlags.toString());

        JSONArray jans = new JSONArray();
        for (IActivity an : activityMap.values()) {
            JSONObject jan = new JSONObject();
            jan.put("id", an.getActivityId().toString());
            jan.put("java-class", an.getClass().getName());

            List<IConnectorDescriptor> inputs = activityInputMap.get(an.getActivityId());
            if (inputs != null) {
                JSONArray jInputs = new JSONArray();
                for (int i = 0; i < inputs.size(); ++i) {
                    JSONObject jInput = new JSONObject();
                    jInput.put("input-port", i);
                    jInput.put("connector-id", inputs.get(i).getConnectorId().toString());
                    jInputs.put(jInput);
                }
                jan.put("inputs", jInputs);
            }

            List<IConnectorDescriptor> outputs = activityOutputMap.get(an.getActivityId());
            if (outputs != null) {
                JSONArray jOutputs = new JSONArray();
                for (int i = 0; i < outputs.size(); ++i) {
                    JSONObject jOutput = new JSONObject();
                    jOutput.put("output-port", i);
                    jOutput.put("connector-id", outputs.get(i).getConnectorId().toString());
                    jOutputs.put(jOutput);
                }
                jan.put("outputs", jOutputs);
            }

            Set<ActivityId> blockers = getBlocked2BlockerMap().get(an.getActivityId());
            if (blockers != null) {
                JSONArray jDeps = new JSONArray();
                for (ActivityId blocker : blockers) {
                    jDeps.put(blocker.toString());
                }
                jan.put("depends-on", jDeps);
            }
            jans.put(jan);
        }
        jplan.put("activities", jans);

        return jplan;
    }
}