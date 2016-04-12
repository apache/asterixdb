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

import java.util.Map;

import org.apache.hyracks.api.dataflow.TaskId;

public class TaskClusterAttempt {
    public enum TaskClusterStatus {
        RUNNING,
        COMPLETED,
        FAILED,
        ABORTED,
    }

    private final TaskCluster taskCluster;

    private final int attempt;

    private Map<TaskId, TaskAttempt> taskAttempts;

    private TaskClusterStatus status;

    private int pendingTaskCounter;

    private long startTime;

    private long endTime;

    public TaskClusterAttempt(TaskCluster taskCluster, int attempt) {
        this.taskCluster = taskCluster;
        this.attempt = attempt;
        startTime = -1;
        endTime = -1;
    }

    public TaskCluster getTaskCluster() {
        return taskCluster;
    }

    public void setTaskAttempts(Map<TaskId, TaskAttempt> taskAttempts) {
        this.taskAttempts = taskAttempts;
    }

    public Map<TaskId, TaskAttempt> getTaskAttempts() {
        return taskAttempts;
    }

    public int getAttempt() {
        return attempt;
    }

    public void setStatus(TaskClusterStatus status) {
        this.status = status;
    }

    public TaskClusterStatus getStatus() {
        return status;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public void initializePendingTaskCounter() {
        pendingTaskCounter = taskAttempts.size();
    }

    public int getPendingTaskCounter() {
        return pendingTaskCounter;
    }

    public int decrementPendingTasksCounter() {
        return --pendingTaskCounter;
    }
}
