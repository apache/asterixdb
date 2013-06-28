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
package edu.uci.ics.hyracks.api.job;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicyAssignmentPolicy;

public class ActivityClusterGraph implements Serializable {
    private static final long serialVersionUID = 1L;

    private int version;

    private final Map<ActivityClusterId, ActivityCluster> activityClusterMap;

    private final Map<ActivityId, ActivityCluster> activityMap;

    private final Map<ConnectorDescriptorId, ActivityCluster> connectorMap;

    private int frameSize;

    private int maxReattempts;

    private IJobletEventListenerFactory jobletEventListenerFactory;

    private IGlobalJobDataFactory globalJobDataFactory;

    private IConnectorPolicyAssignmentPolicy connectorPolicyAssignmentPolicy;

    private boolean useConnectorPolicyForScheduling;

    public ActivityClusterGraph() {
        version = 0;
        activityClusterMap = new HashMap<ActivityClusterId, ActivityCluster>();
        activityMap = new HashMap<ActivityId, ActivityCluster>();
        connectorMap = new HashMap<ConnectorDescriptorId, ActivityCluster>();
        frameSize = 32768;
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

    public JSONObject toJSON() throws JSONException {
        JSONObject acgj = new JSONObject();

        JSONArray acl = new JSONArray();
        for (ActivityCluster ac : activityClusterMap.values()) {
            acl.put(ac.toJSON());
        }
        acgj.put("version", version);
        acgj.put("activity-clusters", acl);
        return acgj;
    }
}