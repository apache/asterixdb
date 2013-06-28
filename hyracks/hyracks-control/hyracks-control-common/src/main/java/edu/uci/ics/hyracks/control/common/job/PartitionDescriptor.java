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

public class PartitionDescriptor implements Serializable {
    private static final long serialVersionUID = 1L;

    private final PartitionId pid;

    private final String nodeId;

    private final TaskAttemptId producingTaskAttemptId;

    private final boolean reusable;

    private PartitionState state;

    public PartitionDescriptor(PartitionId pid, String nodeId, TaskAttemptId producingTaskAttemptId, boolean reusable) {
        this.pid = pid;
        this.nodeId = nodeId;
        this.producingTaskAttemptId = producingTaskAttemptId;
        this.reusable = reusable;
    }

    public PartitionId getPartitionId() {
        return pid;
    }

    public String getNodeId() {
        return nodeId;
    }

    public TaskAttemptId getProducingTaskAttemptId() {
        return producingTaskAttemptId;
    }

    public PartitionState getState() {
        return state;
    }

    public void setState(PartitionState state) {
        this.state = state;
    }

    public boolean isReusable() {
        return reusable;
    }

    @Override
    public String toString() {
        return "[" + pid + ":" + nodeId + ":" + producingTaskAttemptId + (reusable ? "reusable" : "non-reusable") + " "
                + state + "]";
    }
}