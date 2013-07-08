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
package edu.uci.ics.hyracks.control.common.job;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.partitions.PartitionId;

public class PartitionRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private final PartitionId pid;

    private final String requestingNodeId;

    private final TaskAttemptId requestingTaskAttemptId;

    private final PartitionState minState;

    public PartitionRequest(PartitionId pid, String requestingNodeId, TaskAttemptId requestingTaskAttemptId,
            PartitionState minState) {
        this.pid = pid;
        this.requestingNodeId = requestingNodeId;
        this.requestingTaskAttemptId = requestingTaskAttemptId;
        this.minState = minState;
    }

    public PartitionId getPartitionId() {
        return pid;
    }

    public String getNodeId() {
        return requestingNodeId;
    }

    public TaskAttemptId getRequestingTaskAttemptId() {
        return requestingTaskAttemptId;
    }

    public PartitionState getMinimumState() {
        return minState;
    }

    @Override
    public String toString() {
        return "[" + pid + ":" + requestingNodeId + ":" + requestingTaskAttemptId + ":" + minState + "]";
    }
}