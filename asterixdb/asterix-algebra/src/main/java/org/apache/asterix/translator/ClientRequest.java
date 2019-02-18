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

import org.apache.asterix.common.api.ICommonRequestParameters;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class ClientRequest extends BaseClientRequest {

    protected final long creationTime = System.nanoTime();
    protected final Thread executor;
    protected final String statement;
    protected final String clientContextId;
    protected volatile JobId jobId;

    public ClientRequest(ICommonRequestParameters requestParameters) {
        super(requestParameters.getRequestReference());
        this.clientContextId = requestParameters.getClientContextId();
        this.statement = requestParameters.getStatement();
        this.executor = Thread.currentThread();
    }

    @Override
    public String getClientContextId() {
        return clientContextId;
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
    protected ObjectNode asJson() {
        ObjectNode json = super.asJson();
        json.put("jobId", jobId != null ? jobId.toString() : null);
        json.put("statement", statement);
        json.put("clientContextID", clientContextId);
        return json;
    }
}
