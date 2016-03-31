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

import java.util.List;

import org.apache.hyracks.api.dataflow.TaskAttemptId;

public class TaskAttempt {
    public enum TaskStatus {
        INITIALIZED,
        RUNNING,
        COMPLETED,
        FAILED,
        ABORTED,
    }

    private final TaskClusterAttempt tcAttempt;

    private final TaskAttemptId taskId;

    private final Task task;

    private String nodeId;

    private TaskStatus status;

    private List<Exception> exceptions;

    private long startTime;

    private long endTime;

    public TaskAttempt(TaskClusterAttempt tcAttempt, TaskAttemptId taskId, Task task) {
        this.tcAttempt = tcAttempt;
        this.taskId = taskId;
        this.task = task;
        startTime = -1;
        endTime = -1;
    }

    public TaskClusterAttempt getTaskClusterAttempt() {
        return tcAttempt;
    }

    public TaskAttemptId getTaskAttemptId() {
        return taskId;
    }

    public Task getTask() {
        return task;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public List<Exception> getExceptions() {
        return exceptions;
    }

    public void setStatus(TaskStatus status, List<Exception> exceptions) {
        this.status = status;
        this.exceptions = exceptions;
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
}
