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

import org.apache.hyracks.api.job.JobId;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JobProfile extends AbstractProfile {
    private static final long serialVersionUID = 1L;

    private JobId jobId;

    private Map<String, JobletProfile> jobletProfiles;

    public static JobProfile create(DataInput dis) throws IOException {
        JobProfile jobProfile = new JobProfile();
        jobProfile.readFields(dis);
        return jobProfile;
    }

    private JobProfile() {

    }

    public JobProfile(JobId jobId) {
        this.jobId = jobId;
        jobletProfiles = new HashMap<>();
    }

    public JobId getJobId() {
        return jobId;
    }

    public Map<String, JobletProfile> getJobletProfiles() {
        return jobletProfiles;
    }

    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();

        json.put("job-id", jobId.toString());
        populateCounters(json);
        ArrayNode jobletsArray = om.createArrayNode();
        for (JobletProfile p : jobletProfiles.values()) {
            jobletsArray.add(p.toJSON());
        }
        json.set("joblets", jobletsArray);

        return json;
    }

    public void merge(JobProfile other) {
        super.merge(this);
        for (JobletProfile jp : other.jobletProfiles.values()) {
            if (jobletProfiles.containsKey(jp.getNodeId())) {
                jobletProfiles.get(jp.getNodeId()).merge(jp);
            } else {
                jobletProfiles.put(jp.getNodeId(), jp);
            }
        }
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        jobId = JobId.create(input);
        int size = input.readInt();
        jobletProfiles = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String key = input.readUTF();
            JobletProfile value = JobletProfile.create(input);
            jobletProfiles.put(key, value);
        }
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        jobId.writeFields(output);
        output.writeInt(jobletProfiles.size());
        for (Entry<String, JobletProfile> entry : jobletProfiles.entrySet()) {
            output.writeUTF(entry.getKey());
            entry.getValue().writeFields(output);
        }
    }
}
