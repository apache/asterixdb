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
package org.apache.asterix.translator;

import static org.apache.hyracks.api.job.resource.IJobCapacityController.JobSubmissionStatus.QUEUE;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.api.ICommonRequestParameters;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.om.base.AMutableDateTime;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.api.job.resource.IReadOnlyClusterCapacity;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.hyracks.util.StorageUtil;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class ClientRequest extends BaseClientRequest {

    protected static final int MAX_STATEMENT_LENGTH =
            StorageUtil.getIntSizeInBytes(64, StorageUtil.StorageUnit.KILOBYTE);
    protected final long creationTime = System.nanoTime();
    protected final Thread executor;
    protected final String statement;
    protected final String clientContextId;
    protected final JobState jobState;
    protected volatile JobId jobId;
    private volatile String plan; // can be null

    public ClientRequest(ICommonRequestParameters requestParameters) {
        super(requestParameters.getRequestReference());
        this.clientContextId = requestParameters.getClientContextId();
        String stmt = requestParameters.getStatement();
        this.statement = stmt.length() > MAX_STATEMENT_LENGTH ? stmt.substring(0, MAX_STATEMENT_LENGTH) : stmt;
        this.executor = Thread.currentThread();
        this.jobState = new JobState();
    }

    @Override
    public String getClientContextId() {
        return clientContextId;
    }

    public void setPlan(String plan) {
        if (plan != null) {
            this.plan = plan.length() > MAX_STATEMENT_LENGTH ? plan.substring(0, MAX_STATEMENT_LENGTH) : plan;
        }
    }

    public synchronized void setJobId(JobId jobId) {
        this.jobId = jobId;
        setRunning();
    }

    public Thread getExecutor() {
        return executor;
    }

    @Override
    protected void doCancel(ICcApplicationContext appCtx) throws HyracksDataException {
        // if the request has a job, we abort the job and do not interrupt the thread as it will be notified
        // that the job has been cancelled. Otherwise, we interrupt the thread
        if (jobId != null) {
            IHyracksClientConnection hcc = appCtx.getHcc();
            try {
                hcc.cancelJob(jobId);
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        } else if (executor != null) {
            executor.interrupt();
        }
    }

    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public ObjectNode asJson() {
        ObjectNode json = super.asJson();
        return asJson(json, false);
    }

    @Override
    public ObjectNode asRedactedJson() {
        ObjectNode json = super.asRedactedJson();
        return asJson(json, true);
    }

    private ObjectNode asJson(ObjectNode json, boolean redact) {
        putJobDetails(json, redact);
        json.put("statement", redact ? LogRedactionUtil.statement(statement) : statement);
        json.put("clientContextID", clientContextId);
        if (plan != null) {
            json.put("plan", redact ? LogRedactionUtil.userData(plan) : plan);
        }
        return json;
    }

    @Override
    public void jobCreated(JobId jobId, IReadOnlyClusterCapacity requiredClusterCapacity,
            IJobCapacityController.JobSubmissionStatus status) {
        jobState.createTime = System.currentTimeMillis();
        jobState.status = status == QUEUE ? JobStatus.PENDING : JobStatus.RUNNING;
        jobState.requiredCPUs = requiredClusterCapacity.getAggregatedCores();
        jobState.requiredMemoryInBytes = requiredClusterCapacity.getAggregatedMemoryByteSize();
    }

    @Override
    public void jobStarted(JobId jobId) {
        jobState.startTime = System.currentTimeMillis();
        jobState.status = JobStatus.RUNNING;
    }

    @Override
    public void jobFinished(JobId jobId, JobStatus jobStatus, List<Exception> exceptions) {
        jobState.endTime = System.currentTimeMillis();
        jobState.status = jobStatus;
        if (exceptions != null && !exceptions.isEmpty()) {
            jobState.errorMsg = processException(exceptions.get(0));
        }
    }

    protected String processException(Exception e) {
        return ExceptionUtils.unwrap(e).getMessage();
    }

    private void putJobDetails(ObjectNode json, boolean redact) {
        try {
            json.put("jobId", jobId != null ? jobId.toString() : null);
            putJobState(json, jobState, redact);
        } catch (Throwable th) {
            // ignore
        }
    }

    private static void putJobState(ObjectNode json, JobState state, boolean redact) {
        AMutableDateTime dateTime = new AMutableDateTime(0);
        putTime(json, state.createTime, "jobCreateTime", dateTime);
        putTime(json, state.startTime, "jobStartTime", dateTime);
        putTime(json, state.endTime, "jobEndTime", dateTime);
        long queueTime = (state.startTime > 0 ? state.startTime : System.currentTimeMillis()) - state.createTime;
        json.put("jobQueueTime", TimeUnit.MILLISECONDS.toSeconds(queueTime));
        json.put("jobStatus", String.valueOf(state.status));
        json.put("jobRequiredCPUs", state.requiredCPUs);
        json.put("jobRequiredMemory", state.requiredMemoryInBytes);
        if (state.errorMsg != null) {
            json.put("error", redact ? LogRedactionUtil.userData(state.errorMsg) : state.errorMsg);
        }
    }

    private static void putTime(ObjectNode json, long time, String label, AMutableDateTime dateTime) {
        if (time > 0) {
            dateTime.setValue(time);
            json.put(label, dateTime.toSimpleString());
        }
    }

    static class JobState {
        volatile long createTime;
        volatile long startTime;
        volatile long endTime;
        volatile long requiredMemoryInBytes;
        volatile int requiredCPUs;
        volatile JobStatus status;
        volatile String errorMsg;
    }
}
