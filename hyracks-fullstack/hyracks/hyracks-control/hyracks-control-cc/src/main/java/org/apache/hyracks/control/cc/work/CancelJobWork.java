/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.control.cc.work;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.control.common.work.SynchronizableWork;

/**
 * This work cancels a job with the given job id.
 * It is triggered by the cancel call with a job id from the client.
 */
public class CancelJobWork extends SynchronizableWork {
    private final IJobManager jobManager;
    private final JobId jobId;
    private final IResultCallback<Void> callback;

    public CancelJobWork(IJobManager jobManager, JobId jobId, IResultCallback<Void> callback) {
        this.jobId = jobId;
        this.jobManager = jobManager;
        this.callback = callback;
    }

    @Override
    protected void doRun() throws Exception {
        try {
            jobManager.cancel(jobId, callback);
        } catch (Exception e) {
            callback.setException(e);
        }
    }
}
