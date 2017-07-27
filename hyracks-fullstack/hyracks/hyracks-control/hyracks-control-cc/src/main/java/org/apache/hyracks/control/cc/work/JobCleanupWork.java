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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.common.work.AbstractWork;

public class JobCleanupWork extends AbstractWork {
    private static final Logger LOGGER = Logger.getLogger(JobCleanupWork.class.getName());

    private IJobManager jobManager;
    private JobId jobId;
    private JobStatus status;
    private List<Exception> exceptions;

    public JobCleanupWork(IJobManager jobManager, JobId jobId, JobStatus status, List<Exception> exceptions) {
        this.jobManager = jobManager;
        this.jobId = jobId;
        this.status = status;
        this.exceptions = exceptions;
    }

    @Override
    public void run() {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Cleanup for JobRun with id: " + jobId);
        }
        try {
            JobRun jobRun = jobManager.get(jobId);
            jobManager.prepareComplete(jobRun, status, exceptions);
        } catch (HyracksException e) {
            // Fail the job with the caught exception during final completion.
            JobRun run = jobManager.get(jobId);
            List<Exception> completionException = new ArrayList<>();
            if (run.getExceptions() != null && !run.getExceptions().isEmpty()) {
                completionException.addAll(run.getExceptions());
            }
            completionException.add(0, e);
            run.setStatus(JobStatus.FAILURE, completionException);
        }
    }

    @Override
    public String toString() {
        return getName() + ": JobId@" + jobId + " Status@" + status
                + (exceptions == null ? "" : " Exceptions@" + exceptions);
    }
}
