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
package edu.uci.ics.hyracks.control.cc.job;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.control.cc.scheduler.IActivityClusterStateMachine;
import edu.uci.ics.hyracks.control.common.job.dataflow.IConnectorPolicy;

public class ActivityCluster {
    private final JobRun jobRun;

    private final Set<ActivityNodeId> activities;

    private final Set<ActivityCluster> dependencies;

    private final Set<ActivityCluster> dependents;

    private final Map<ActivityNodeId, Task[]> taskStateMap;

    private TaskCluster[] taskClusters;

    private IActivityClusterStateMachine acsm;

    private Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies;

    public ActivityCluster(JobRun jobRun, Set<ActivityNodeId> activities) {
        this.jobRun = jobRun;
        this.activities = activities;
        dependencies = new HashSet<ActivityCluster>();
        dependents = new HashSet<ActivityCluster>();
        taskStateMap = new HashMap<ActivityNodeId, Task[]>();
    }

    public Set<ActivityNodeId> getActivities() {
        return activities;
    }

    public void addDependency(ActivityCluster stage) {
        dependencies.add(stage);
    }

    public void addDependent(ActivityCluster stage) {
        dependents.add(stage);
    }

    public Set<ActivityCluster> getDependencies() {
        return dependencies;
    }

    public Map<ActivityNodeId, Task[]> getTaskMap() {
        return taskStateMap;
    }

    public TaskCluster[] getTaskClusters() {
        return taskClusters;
    }

    public void setTaskClusters(TaskCluster[] taskClusters) {
        this.taskClusters = taskClusters;
    }

    public IActivityClusterStateMachine getStateMachine() {
        return acsm;
    }

    public void setStateMachine(IActivityClusterStateMachine acsm) {
        this.acsm = acsm;
    }

    public JobRun getJobRun() {
        return jobRun;
    }

    public int getMaxTaskClusterAttempts() {
        return 1;
    }

    public void notifyTaskClusterFailure(TaskClusterAttempt tcAttempt, Exception exception) throws HyracksException {
        acsm.notifyTaskClusterFailure(tcAttempt, exception);
    }

    public void notifyActivityClusterComplete() throws HyracksException {
        jobRun.getStateMachine().notifyActivityClusterComplete(this);
    }

    public void setConnectorPolicyMap(Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies) {
        this.connectorPolicies = connectorPolicies;
    }

    public Map<ConnectorDescriptorId, IConnectorPolicy> getConnectorPolicyMap() {
        return connectorPolicies;
    }
}