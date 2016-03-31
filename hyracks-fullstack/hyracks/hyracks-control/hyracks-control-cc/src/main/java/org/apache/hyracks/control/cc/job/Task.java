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
package org.apache.hyracks.control.cc.job;

import java.util.HashSet;
import java.util.Set;

import org.apache.hyracks.api.dataflow.TaskId;

public class Task {
    private final TaskId taskId;

    private final ActivityPlan activityPlan;

    private final Set<TaskId> dependencies;

    private final Set<TaskId> dependents;

    private TaskCluster taskCluster;

    public Task(TaskId taskId, ActivityPlan activityPlan) {
        this.taskId = taskId;
        this.activityPlan = activityPlan;
        this.dependencies = new HashSet<TaskId>();
        this.dependents = new HashSet<TaskId>();
    }

    public TaskId getTaskId() {
        return taskId;
    }

    public ActivityPlan getActivityPlan() {
        return activityPlan;
    }

    public Set<TaskId> getDependencies() {
        return dependencies;
    }

    public Set<TaskId> getDependents() {
        return dependents;
    }

    public TaskCluster getTaskCluster() {
        return taskCluster;
    }

    public void setTaskCluster(TaskCluster taskCluster) {
        this.taskCluster = taskCluster;
    }

    @Override
    public String toString() {
        return String.valueOf(taskId);
    }
}
