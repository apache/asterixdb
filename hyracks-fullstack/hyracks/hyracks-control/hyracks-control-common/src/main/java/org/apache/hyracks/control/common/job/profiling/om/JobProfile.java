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

import static org.apache.hyracks.api.job.profiling.NoOpOperatorStats.INVALID_ODID;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.job.profiling.IStatsCollector;
import org.apache.hyracks.api.job.profiling.NoOpOperatorStats;
import org.apache.hyracks.api.job.profiling.OperatorStats;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JobProfile extends AbstractProfile {
    private static final long serialVersionUID = 2L;

    private JobId jobId;
    private long createTime;
    private long startTime;
    private String startTimeZoneId;
    private long endTime;

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

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setStartTimeZoneId(String startTimeZoneId) {
        this.startTimeZoneId = startTimeZoneId;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getQueueWaitTimeInNanos() {
        return TimeUnit.MILLISECONDS.toNanos(startTime - createTime);
    }

    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();

        json.put("job-id", jobId.toString());
        json.put("create-time", createTime);
        json.put("start-time", startTime);
        json.put("queued-time", startTime - createTime);
        json.put("end-time", endTime);
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
        createTime = input.readLong();
        startTime = input.readLong();
        endTime = input.readLong();
        startTimeZoneId = input.readUTF();
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
        output.writeLong(createTime);
        output.writeLong(startTime);
        output.writeLong(endTime);
        output.writeUTF(startTimeZoneId);
        output.writeInt(jobletProfiles.size());
        for (Entry<String, JobletProfile> entry : jobletProfiles.entrySet()) {
            output.writeUTF(entry.getKey());
            entry.getValue().writeFields(output);
        }
    }

    public List<IOperatorStats> getAggregatedStats(List<String> operatorNames) {
        if (jobletProfiles == null || operatorNames == null || operatorNames.isEmpty()) {
            return null;
        }
        // gather final task attempts for each task
        Map<TaskId, TaskProfile> taskProfileMap = new HashMap<>();
        for (JobletProfile jobletProfile : jobletProfiles.values()) {
            for (TaskProfile taskProfile : jobletProfile.getTaskProfiles().values()) {
                TaskAttemptId taskAttemptId = taskProfile.getTaskId();
                TaskId taskId = taskAttemptId.getTaskId();
                TaskProfile existingProfile = taskProfileMap.get(taskId);
                if (existingProfile == null || taskAttemptId.getAttempt() > existingProfile.getTaskId().getAttempt()) {
                    taskProfileMap.put(taskId, taskProfile);
                }
            }
        }
        // compute aggregated counts
        int n = operatorNames.size();
        IOperatorStats[] outStats = new IOperatorStats[n];
        for (TaskProfile taskProfile : taskProfileMap.values()) {
            IStatsCollector statsCollector = taskProfile.getStatsCollector();
            for (int i = 0; i < n; i++) {
                String operatorName = operatorNames.get(i);
                IOperatorStats opTaskStats = statsCollector.getOperatorStats(operatorName);
                if (opTaskStats.equals(NoOpOperatorStats.INSTANCE)) {
                    continue;
                }
                IOperatorStats opOutStats = outStats[i];
                if (opOutStats == null) {
                    opOutStats = new OperatorStats(operatorName, INVALID_ODID);
                    outStats[i] = opOutStats;
                }
                opOutStats.updateFrom(opTaskStats);
            }
        }
        return Arrays.asList(outStats);
    }

}
