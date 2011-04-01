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
package edu.uci.ics.hyracks.control.common.job.profiling.om;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;

public class TaskProfile extends AbstractProfile {
    private static final long serialVersionUID = 1L;

    private final TaskAttemptId taskAttemptId;

    public TaskProfile(TaskAttemptId taskAttemptId) {
        this.taskAttemptId = taskAttemptId;
    }

    public TaskAttemptId getTaskId() {
        return taskAttemptId;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        json.put("type", "task-profile");
        json.put("activity-id", taskAttemptId.getTaskId().getActivityId().toString());
        json.put("partition", taskAttemptId.getTaskId().getPartition());
        json.put("attempt", taskAttemptId.getAttempt());
        populateCounters(json);

        return json;
    }
}