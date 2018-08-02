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

import java.util.Collections;
import java.util.List;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.control.common.work.SynchronizableWork;

public class WaitForJobCompletionWork extends SynchronizableWork {
    private final ClusterControllerService ccs;
    private final JobId jobId;
    private final IResultCallback<Object> callback;

    public WaitForJobCompletionWork(ClusterControllerService ccs, JobId jobId, IResultCallback<Object> callback) {
        this.ccs = ccs;
        this.jobId = jobId;
        this.callback = callback;
    }

    @Override
    protected void doRun() throws Exception {
        IJobManager jobManager = ccs.getJobManager();
        final JobRun jobRun = jobManager.get(jobId);
        if (jobRun != null) {
            ccs.getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.currentThread()
                                .setName(Thread.currentThread().getName() + " : WaitForCompletionForJobId: " + jobId);
                        jobRun.waitForCompletion();
                        callback.setValue(null);
                    } catch (Exception e) {
                        callback.setException(e);
                    }
                }
            });
        } else {
            // Couldn't find jobRun
            List<Exception> exceptionHistory = jobManager.getExceptionHistory(jobId);
            List<Exception> exceptions;
            if (exceptionHistory == null) {
                // couldn't be found
                exceptions = Collections
                        .singletonList(HyracksDataException.create(ErrorCode.JOB_HAS_BEEN_CLEARED_FROM_HISTORY, jobId));

            } else {
                exceptions = exceptionHistory;
            }
            ccs.getExecutor().execute(() -> {
                if (!exceptions.isEmpty()) {
                    /*
                     * only report the first exception because IResultCallback will only throw one exception
                     * anyway
                     */
                    callback.setException(exceptions.get(0));
                } else {
                    callback.setValue(null);
                }
            });
        }
    }
}
