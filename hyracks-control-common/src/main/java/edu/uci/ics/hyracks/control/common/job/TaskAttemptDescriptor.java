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
package edu.uci.ics.hyracks.control.common.job;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;

public class TaskAttemptDescriptor implements Serializable {
    private static final long serialVersionUID = 1L;

    private final TaskAttemptId taId;

    private final int nPartitions;

    private final int[] nInputPartitions;

    private final int[] nOutputPartitions;

    public TaskAttemptDescriptor(TaskAttemptId taId, int nPartitions, int[] nInputPartitions, int[] nOutputPartitions) {
        this.taId = taId;
        this.nPartitions = nPartitions;
        this.nInputPartitions = nInputPartitions;
        this.nOutputPartitions = nOutputPartitions;
    }

    public TaskAttemptId getTaskAttemptId() {
        return taId;
    }

    public int getPartitionCount() {
        return nPartitions;
    }

    public int[] getInputPartitionCounts() {
        return nInputPartitions;
    }

    public int[] getOutputPartitionCounts() {
        return nOutputPartitions;
    }
}