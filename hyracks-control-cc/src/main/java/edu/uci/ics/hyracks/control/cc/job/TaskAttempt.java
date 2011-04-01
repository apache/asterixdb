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

import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;

public class TaskAttempt {
    public enum TaskStatus {
        INITIALIZED,
        RUNNING,
        COMPLETED,
        FAILED,
        ABORTED,
    }

    private final TaskAttemptId taskId;

    private final TaskState taskState;

    private String nodeId;

    private TaskStatus status;

    private Exception exception;

    public TaskAttempt(TaskAttemptId taskId, TaskState taskState) {
        this.taskId = taskId;
        this.taskState = taskState;
    }

    public TaskAttemptId getTaskAttemptId() {
        return taskId;
    }

    public TaskState getTaskState() {
        return taskState;
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

    public Exception getException() {
        return exception;
    }

    public void setStatus(TaskStatus status, Exception exception) {
        this.status = status;
        this.exception = exception;
    }

    public void notifyTaskComplete() throws HyracksException {
        taskState.getTaskCluster().notifyTaskComplete(this);
    }

    public void notifyTaskFailure(Exception exception) throws HyracksException {
        taskState.getTaskCluster().notifyTaskFailure(this, exception);
    }
}