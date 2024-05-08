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

import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.api.ICommonRequestParameters;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.om.base.AMutableDateTime;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.resource.IClusterCapacity;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.job.JobRun;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class ClientRequest extends BaseClientRequest {

    protected final long creationTime = System.nanoTime();
    protected final Thread executor;
    protected final String statement;
    protected final ClusterControllerService ccService;
    protected final String clientContextId;
    protected volatile JobId jobId;
    private String plan; // can be null

    public ClientRequest(ICommonRequestParameters requestParameters, ICcApplicationContext appCtx) {
        super(requestParameters.getRequestReference());
        this.clientContextId = requestParameters.getClientContextId();
        this.statement = requestParameters.getStatement();
        this.executor = Thread.currentThread();
        this.ccService = (ClusterControllerService) appCtx.getServiceContext().getControllerService();
    }

    @Override
    public String getClientContextId() {
        return clientContextId;
    }

    public void setPlan(String plan) {
        this.plan = plan;
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
        putJobDetails(json);
        json.put("statement", statement);
        json.put("clientContextID", clientContextId);
        if (plan != null) {
            json.put("plan", plan);
        }
        return json;
    }

    private void putJobDetails(ObjectNode json) {
        if (jobId == null) {
            json.putNull("jobId");
        } else {
            try {
                json.put("jobId", jobId.toString());
                JobRun jobRun = ccService.getJobManager().get(jobId);
                if (jobRun != null) {
                    json.put("jobStatus", String.valueOf(jobRun.getStatus()));
                    putJobRequiredResources(json, jobRun);
                    putTimes(json, jobRun);
                }
            } catch (Throwable th) {
                // ignore
            }
        }
    }

    private static void putTimes(ObjectNode json, JobRun jobRun) {
        AMutableDateTime dateTime = new AMutableDateTime(0);
        putTime(json, jobRun.getCreateTime(), "jobCreateTime", dateTime);
        putTime(json, jobRun.getStartTime(), "jobStartTime", dateTime);
        putTime(json, jobRun.getEndTime(), "jobEndTime", dateTime);
        json.put("jobQueueTime", TimeUnit.MILLISECONDS.toSeconds(jobRun.getQueueWaitTimeInMillis()));
    }

    private static void putJobRequiredResources(ObjectNode json, JobRun jobRun) {
        IClusterCapacity jobCapacity = jobRun.getJobSpecification().getRequiredClusterCapacity();
        if (jobCapacity != null) {
            json.put("jobRequiredCPUs", jobCapacity.getAggregatedCores());
            json.put("jobRequiredMemory", jobCapacity.getAggregatedMemoryByteSize());
        }
    }

    private static void putTime(ObjectNode json, long time, String label, AMutableDateTime dateTime) {
        if (time > 0) {
            dateTime.setValue(time);
            json.put(label, dateTime.toSimpleString());
        }
    }
}
