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
package edu.uci.ics.hyracks.control.common.job.profiling.om;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;

public class JobletProfile extends AbstractProfile {
    private static final long serialVersionUID = 1L;

    private final String nodeId;

    private final Map<TaskAttemptId, TaskProfile> taskProfiles;

    public JobletProfile(String nodeId) {
        this.nodeId = nodeId;
        taskProfiles = new HashMap<TaskAttemptId, TaskProfile>();
    }

    public String getNodeId() {
        return nodeId;
    }

    public Map<TaskAttemptId, TaskProfile> getTaskProfiles() {
        return taskProfiles;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        json.put("node-id", nodeId.toString());
        populateCounters(json);
        JSONArray tasks = new JSONArray();
        for (TaskProfile p : taskProfiles.values()) {
            tasks.put(p.toJSON());
        }
        json.put("tasks", tasks);

        return json;
    }

    public void merge(JobletProfile jp) {
        super.merge(this);
        for (TaskProfile tp : jp.taskProfiles.values()) {
            if (taskProfiles.containsKey(tp.getTaskId())) {
                taskProfiles.get(tp.getTaskId()).merge(tp);
            } else {
                taskProfiles.put(tp.getTaskId(), tp);
            }
        }
    }
}