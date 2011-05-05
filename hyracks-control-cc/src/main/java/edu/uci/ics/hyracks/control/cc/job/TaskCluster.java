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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.partitions.PartitionId;

public class TaskCluster {
    private final ActivityCluster activityCluster;

    private final Task[] tasks;

    private final Set<PartitionId> producedPartitions;

    private final Set<PartitionId> requiredPartitions;

    private final Set<TaskCluster> blockers;

    private final Set<TaskCluster> dependencies;

    private final List<TaskClusterAttempt> taskClusterAttempts;

    public TaskCluster(ActivityCluster activityCluster, Task[] tasks) {
        this.activityCluster = activityCluster;
        this.tasks = tasks;
        this.producedPartitions = new HashSet<PartitionId>();
        this.requiredPartitions = new HashSet<PartitionId>();
        this.blockers = new HashSet<TaskCluster>();
        this.dependencies = new HashSet<TaskCluster>();
        taskClusterAttempts = new ArrayList<TaskClusterAttempt>();
    }

    public Task[] getTasks() {
        return tasks;
    }

    public Set<TaskCluster> getDependencies() {
        return dependencies;
    }

    public Set<PartitionId> getProducedPartitions() {
        return producedPartitions;
    }

    public Set<PartitionId> getRequiredPartitions() {
        return requiredPartitions;
    }

    public Set<TaskCluster> getBlockers() {
        return blockers;
    }

    public List<TaskClusterAttempt> getAttempts() {
        return taskClusterAttempts;
    }

    public void notifyTaskComplete(TaskAttempt ta) throws HyracksException {
        activityCluster.getStateMachine().notifyTaskComplete(ta);
    }

    public void notifyTaskFailure(TaskAttempt ta, Exception exception) throws HyracksException {
        activityCluster.getStateMachine().notifyTaskFailure(ta, exception);
    }
}