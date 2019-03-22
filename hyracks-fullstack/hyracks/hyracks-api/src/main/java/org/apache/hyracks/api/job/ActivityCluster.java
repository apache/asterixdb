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
package org.apache.hyracks.api.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.connectors.IConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ActivityCluster implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ActivityClusterGraph acg;

    private final ActivityClusterId id;

    private final List<IActivity> roots;

    private final Map<ActivityId, IActivity> activities;

    private final Map<ConnectorDescriptorId, IConnectorDescriptor> connectors;

    private final Map<ConnectorDescriptorId, RecordDescriptor> connectorRecordDescriptorMap;

    private final Map<ActivityId, List<IConnectorDescriptor>> activityInputMap;

    private final Map<ActivityId, List<IConnectorDescriptor>> activityOutputMap;

    private final Map<ConnectorDescriptorId, Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>>> connectorActivityMap;

    private final Map<ActivityId, Set<ActivityId>> blocked2blockerMap;

    private final List<ActivityCluster> dependencies;

    private IConnectorPolicyAssignmentPolicy cpap;

    public ActivityCluster(ActivityClusterGraph acg, ActivityClusterId id) {
        this.acg = acg;
        this.id = id;
        roots = new ArrayList<IActivity>();
        activities = new HashMap<ActivityId, IActivity>();
        connectors = new HashMap<ConnectorDescriptorId, IConnectorDescriptor>();
        connectorRecordDescriptorMap = new HashMap<ConnectorDescriptorId, RecordDescriptor>();
        activityInputMap = new HashMap<ActivityId, List<IConnectorDescriptor>>();
        activityOutputMap = new HashMap<ActivityId, List<IConnectorDescriptor>>();
        connectorActivityMap =
                new HashMap<ConnectorDescriptorId, Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>>>();
        blocked2blockerMap = new HashMap<ActivityId, Set<ActivityId>>();
        dependencies = new ArrayList<ActivityCluster>();
    }

    public ActivityClusterGraph getActivityClusterGraph() {
        return acg;
    }

    public ActivityClusterId getId() {
        return id;
    }

    public void addRoot(IActivity activity) {
        roots.add(activity);
    }

    public void addActivity(IActivity activity) {
        activities.put(activity.getActivityId(), activity);
    }

    public void addConnector(IConnectorDescriptor connector) {
        connectors.put(connector.getConnectorId(), connector);
    }

    public void connect(IConnectorDescriptor connector, IActivity producerActivity, int producerPort,
            IActivity consumerActivity, int consumerPort, RecordDescriptor recordDescriptor) {
        if (!activities.containsKey(producerActivity.getActivityId())
                || !activities.containsKey(consumerActivity.getActivityId())) {
            throw new IllegalStateException("Connected Activities belong to different Activity Clusters: "
                    + producerActivity.getActivityId() + " and " + consumerActivity.getActivityId());
        }
        insertIntoIndexedMap(activityInputMap, consumerActivity.getActivityId(), consumerPort, connector);
        insertIntoIndexedMap(activityOutputMap, producerActivity.getActivityId(), producerPort, connector);
        connectorActivityMap.put(connector.getConnectorId(),
                Pair.of(Pair.of(producerActivity, producerPort), Pair.of(consumerActivity, consumerPort)));
        connectorRecordDescriptorMap.put(connector.getConnectorId(), recordDescriptor);
    }

    public List<IActivity> getRoots() {
        return roots;
    }

    public Map<ActivityId, IActivity> getActivityMap() {
        return activities;
    }

    public Map<ConnectorDescriptorId, IConnectorDescriptor> getConnectorMap() {
        return connectors;
    }

    public Map<ConnectorDescriptorId, RecordDescriptor> getConnectorRecordDescriptorMap() {
        return connectorRecordDescriptorMap;
    }

    public Map<ConnectorDescriptorId, Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>>> getConnectorActivityMap() {
        return connectorActivityMap;
    }

    public Map<ActivityId, List<IConnectorDescriptor>> getActivityInputMap() {
        return activityInputMap;
    }

    public Map<ActivityId, List<IConnectorDescriptor>> getActivityOutputMap() {
        return activityOutputMap;
    }

    public ActivityId getConsumerActivity(ConnectorDescriptorId cdId) {
        Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>> connEdge = connectorActivityMap.get(cdId);
        return connEdge.getRight().getLeft().getActivityId();
    }

    public ActivityId getProducerActivity(ConnectorDescriptorId cdId) {
        Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>> connEdge = connectorActivityMap.get(cdId);
        return connEdge.getLeft().getLeft().getActivityId();
    }

    public Map<ActivityId, Set<ActivityId>> getBlocked2BlockerMap() {
        return blocked2blockerMap;
    }

    public List<ActivityCluster> getDependencies() {
        return dependencies;
    }

    public IConnectorPolicyAssignmentPolicy getConnectorPolicyAssignmentPolicy() {
        return cpap;
    }

    public void setConnectorPolicyAssignmentPolicy(IConnectorPolicyAssignmentPolicy cpap) {
        this.cpap = cpap;
    }

    private <T> void extend(List<T> list, int index) {
        int n = list.size();
        for (int i = n; i <= index; ++i) {
            list.add(null);
        }
    }

    private <K, V> void insertIntoIndexedMap(Map<K, List<V>> map, K key, int index, V value) {
        List<V> vList = map.get(key);
        if (vList == null) {
            vList = new ArrayList<V>();
            map.put(key, vList);
        }
        extend(vList, index);
        vList.set(index, value);
    }

    public JsonNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ArrayNode jans = om.createArrayNode();
        ObjectNode jac = om.createObjectNode();
        for (IActivity an : activities.values()) {
            ObjectNode jan = om.createObjectNode();
            jan.put("id", an.getActivityId().toString());
            jan.put("java-class", an.getClass().getName());

            List<IConnectorDescriptor> inputs = activityInputMap.get(an.getActivityId());
            if (inputs != null) {
                ArrayNode jInputs = om.createArrayNode();
                for (int i = 0; i < inputs.size(); ++i) {
                    ObjectNode jInput = om.createObjectNode();
                    jInput.put("input-port", i);
                    jInput.put("connector-id", inputs.get(i).getConnectorId().toString());
                    jInputs.add(jInput);
                }
                jan.set("inputs", jInputs);
            }

            List<IConnectorDescriptor> outputs = activityOutputMap.get(an.getActivityId());
            if (outputs != null) {
                ArrayNode jOutputs = om.createArrayNode();
                for (int i = 0; i < outputs.size(); ++i) {
                    ObjectNode jOutput = om.createObjectNode();
                    jOutput.put("output-port", i);
                    jOutput.put("connector-id", outputs.get(i).getConnectorId().toString());
                    jOutputs.add(jOutput);
                }
                jan.set("outputs", jOutputs);
            }

            Set<ActivityId> blockers = getBlocked2BlockerMap().get(an.getActivityId());
            if (blockers != null) {
                ArrayNode jDeps = om.createArrayNode();
                for (ActivityId blocker : blockers) {
                    jDeps.add(blocker.toString());
                }
                jan.set("depends-on", jDeps);
            }
            jans.add(jan);
        }
        jac.set("activities", jans);

        return jac;
    }
}
