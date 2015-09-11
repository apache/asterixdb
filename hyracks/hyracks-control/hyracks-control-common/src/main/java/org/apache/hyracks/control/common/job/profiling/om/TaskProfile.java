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

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.control.common.job.profiling.counters.MultiResolutionEventProfiler;

public class TaskProfile extends AbstractProfile {
    private static final long serialVersionUID = 1L;

    private TaskAttemptId taskAttemptId;

    private Map<PartitionId, PartitionProfile> partitionSendProfile;

    public static TaskProfile create(DataInput dis) throws IOException {
        TaskProfile taskProfile = new TaskProfile();
        taskProfile.readFields(dis);
        return taskProfile;
    }

    private TaskProfile() {

    }

    public TaskProfile(TaskAttemptId taskAttemptId, Map<PartitionId, PartitionProfile> partitionSendProfile) {
        this.taskAttemptId = taskAttemptId;
        this.partitionSendProfile = new HashMap<PartitionId, PartitionProfile>(partitionSendProfile);
    }

    public TaskAttemptId getTaskId() {
        return taskAttemptId;
    }

    public Map<PartitionId, PartitionProfile> getPartitionSendProfile() {
        return partitionSendProfile;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        json.put("activity-id", taskAttemptId.getTaskId().getActivityId().toString());
        json.put("partition", taskAttemptId.getTaskId().getPartition());
        json.put("attempt", taskAttemptId.getAttempt());
        if (partitionSendProfile != null) {
            JSONArray pspArray = new JSONArray();
            for (PartitionProfile pp : partitionSendProfile.values()) {
                JSONObject ppObj = new JSONObject();
                PartitionId pid = pp.getPartitionId();
                JSONObject pidObj = new JSONObject();
                pidObj.put("job-id", pid.getJobId());
                pidObj.put("connector-id", pid.getConnectorDescriptorId());
                pidObj.put("sender-index", pid.getSenderIndex());
                pidObj.put("receiver-index", pid.getReceiverIndex());
                ppObj.put("partition-id", pidObj);
                ppObj.put("open-time", pp.getOpenTime());
                ppObj.put("close-time", pp.getCloseTime());
                MultiResolutionEventProfiler samples = pp.getSamples();
                ppObj.put("offset", samples.getOffset());
                int resolution = samples.getResolution();
                int sampleCount = samples.getCount();
                JSONArray ftA = new JSONArray();
                int[] ft = samples.getSamples();
                for (int i = 0; i < sampleCount; ++i) {
                    ftA.put(ft[i]);
                }
                ppObj.put("frame-times", ftA);
                ppObj.put("resolution", resolution);
                pspArray.put(ppObj);
            }
            json.put("partition-send-profile", pspArray);
        }
        populateCounters(json);

        return json;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        taskAttemptId = TaskAttemptId.create(input);
        int size = input.readInt();
        partitionSendProfile = new HashMap<PartitionId, PartitionProfile>();
        for (int i = 0; i < size; i++) {
            PartitionId key = PartitionId.create(input);
            PartitionProfile value = PartitionProfile.create(input);
            partitionSendProfile.put(key, value);
        }
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        super.writeFields(output);
        taskAttemptId.writeFields(output);
        output.writeInt(partitionSendProfile.size());
        for (Entry<PartitionId, PartitionProfile> entry : partitionSendProfile.entrySet()) {
            entry.getKey().writeFields(output);
            entry.getValue().writeFields(output);
        }
    }
}