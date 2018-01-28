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
package org.apache.hyracks.control.common.job.profiling.om;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hyracks.api.dataflow.TaskAttemptId;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JobletProfile extends AbstractProfile {
    private static final long serialVersionUID = 1L;

    private String nodeId;

    private Map<TaskAttemptId, TaskProfile> taskProfiles;

    public static JobletProfile create(DataInput dis) throws IOException {
        JobletProfile jobletProfile = new JobletProfile();
        jobletProfile.readFields(dis);
        return jobletProfile;
    }

    private JobletProfile() {

    }

    public JobletProfile(String nodeId) {
        this.nodeId = nodeId;
        taskProfiles = new HashMap<>();
    }

    public String getNodeId() {
        return nodeId;
    }

    public Map<TaskAttemptId, TaskProfile> getTaskProfiles() {
        return taskProfiles;
    }

    @Override
    public ObjectNode toJSON() {

        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();

        json.put("node-id", nodeId);
        populateCounters(json);
        ArrayNode tasks = om.createArrayNode();
        for (TaskProfile p : taskProfiles.values()) {
            tasks.add(p.toJSON());
        }
        json.set("tasks", tasks);

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

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        nodeId = input.readUTF();
        int size = input.readInt();
        taskProfiles = new HashMap<>();
        for (int i = 0; i < size; i++) {
            TaskAttemptId key = TaskAttemptId.create(input);
            TaskProfile value = TaskProfile.create(input);
            taskProfiles.put(key, value);
        }
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        super.writeFields(output);
        output.writeUTF(nodeId);
        output.writeInt(taskProfiles.size());
        for (Entry<TaskAttemptId, TaskProfile> entry : taskProfiles.entrySet()) {
            entry.getKey().writeFields(output);
            entry.getValue().writeFields(output);
        }
    }
}
