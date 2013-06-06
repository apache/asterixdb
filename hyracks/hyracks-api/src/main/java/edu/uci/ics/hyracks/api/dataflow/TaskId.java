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
package edu.uci.ics.hyracks.api.dataflow;

import java.io.Serializable;

public final class TaskId implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ActivityId activityId;

    private final int partition;

    public TaskId(ActivityId activityId, int partition) {
        this.activityId = activityId;
        this.partition = partition;
    }

    public ActivityId getActivityId() {
        return activityId;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TaskId)) {
            return false;
        }
        TaskId oTaskId = (TaskId) o;
        return oTaskId.partition == partition && oTaskId.activityId.equals(activityId);
    }

    @Override
    public int hashCode() {
        return activityId.hashCode() + partition;
    }

    @Override
    public String toString() {
        return "TID:" + activityId + ":" + partition;
    }

    public static TaskId parse(String str) {
        if (str.startsWith("TID:")) {
            str = str.substring(4);
            int idIdx = str.lastIndexOf(':');
            return new TaskId(ActivityId.parse(str.substring(0, idIdx)), Integer.parseInt(str.substring(idIdx + 1)));
        }
        throw new IllegalArgumentException("Unable to parse: " + str);
    }
}