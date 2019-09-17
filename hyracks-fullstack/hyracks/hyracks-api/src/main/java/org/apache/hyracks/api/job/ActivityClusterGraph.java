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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.connectors.IConnectorPolicyAssignmentPolicy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ActivityClusterGraph implements Serializable {
    private static final long serialVersionUID = 1L;

    private int version;

    private final Map<ActivityClusterId, ActivityCluster> activityClusterMap;

    private final Map<ActivityId, ActivityCluster> activityMap;

    private final Map<ConnectorDescriptorId, ActivityCluster> connectorMap;

    private int frameSize;

    private long maxWarnings;

    private int maxReattempts;

    private IJobletEventListenerFactory jobletEventListenerFactory;

    private IGlobalJobDataFactory globalJobDataFactory;

    private IConnectorPolicyAssignmentPolicy connectorPolicyAssignmentPolicy;

    private boolean useConnectorPolicyForScheduling;

    private boolean reportTaskDetails;

    public ActivityClusterGraph() {
        version = 0;
        activityClusterMap = new HashMap<ActivityClusterId, ActivityCluster>();
        activityMap = new HashMap<ActivityId, ActivityCluster>();
        connectorMap = new HashMap<ConnectorDescriptorId, ActivityCluster>();
        frameSize = 32768;
        reportTaskDetails = true;
    }

    public Map<ActivityId, ActivityCluster> getActivityMap() {
        return activityMap;
    }

    public Map<ConnectorDescriptorId, ActivityCluster> getConnectorMap() {
        return connectorMap;
    }

    public Map<ActivityClusterId, ActivityCluster> getActivityClusterMap() {
        return activityClusterMap;
    }

    public void addActivityClusters(Collection<ActivityCluster> newActivityClusters) {
        for (ActivityCluster ac : newActivityClusters) {
            activityClusterMap.put(ac.getId(), ac);
            for (ActivityId aid : ac.getActivityMap().keySet()) {
                activityMap.put(aid, ac);
            }
            for (ConnectorDescriptorId cid : ac.getConnectorMap().keySet()) {
                connectorMap.put(cid, ac);
            }
        }
        ++version;
    }

    public int getVersion() {
        return version;
    }

    public void setFrameSize(int frameSize) {
        this.frameSize = frameSize;
    }

    public int getFrameSize() {
        return frameSize;
    }

    public void setMaxWarnings(long maxWarnings) {
        this.maxWarnings = maxWarnings;
    }

    public long getMaxWarnings() {
        return maxWarnings;
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

    public IConnectorPolicyAssignmentPolicy getConnectorPolicyAssignmentPolicy() {
        return connectorPolicyAssignmentPolicy;
    }

    public void setConnectorPolicyAssignmentPolicy(IConnectorPolicyAssignmentPolicy connectorPolicyAssignmentPolicy) {
        this.connectorPolicyAssignmentPolicy = connectorPolicyAssignmentPolicy;
    }

    public boolean isUseConnectorPolicyForScheduling() {
        return useConnectorPolicyForScheduling;
    }

    public void setUseConnectorPolicyForScheduling(boolean useConnectorPolicyForScheduling) {
        this.useConnectorPolicyForScheduling = useConnectorPolicyForScheduling;
    }

    public boolean isReportTaskDetails() {
        return reportTaskDetails;
    }

    public void setReportTaskDetails(boolean reportTaskDetails) {
        this.reportTaskDetails = reportTaskDetails;
    }

    public List<IConnectorDescriptor> getActivityInputs(ActivityId activityId) {
        ActivityCluster ac = activityMap.get(activityId);
        return ac.getActivityInputMap().get(activityId);
    }

    public ActivityId getProducerActivity(ConnectorDescriptorId cid) {
        ActivityCluster ac = connectorMap.get(cid);
        return ac.getProducerActivity(cid);
    }

    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode acgj = om.createObjectNode();
        ArrayNode acl = om.createArrayNode();
        for (ActivityCluster ac : activityClusterMap.values()) {
            acl.add(ac.toJSON());
        }
        acgj.put("version", version);
        acgj.set("activity-clusters", acl);
        return acgj;
    }
}
