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
package org.apache.hyracks.control.cc.work;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobInfo;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.control.common.work.SynchronizableWork;

public class GetJobInfoWork extends SynchronizableWork {
    private final IJobManager jobManager;
    private final JobId jobId;
    private final IResultCallback<JobInfo> callback;

    public GetJobInfoWork(IJobManager jobManager, JobId jobId, IResultCallback<JobInfo> callback) {
        this.jobManager = jobManager;
        this.jobId = jobId;
        this.callback = callback;
    }

    @Override
    protected void doRun() throws Exception {
        try {
            JobRun run = jobManager.get(jobId);
            JobInfo info =
                    (run != null) ? new JobInfo(run.getJobId(), run.getStatus(), run.getOperatorLocations()) : null;
            callback.setValue(info);
        } catch (Exception e) {
            callback.setException(e);
        }
    }
}
