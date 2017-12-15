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

package org.apache.hyracks.control.cc.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.util.annotations.NotThreadSafe;
import org.apache.hyracks.util.annotations.ThreadSafetyGuaranteedBy;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * An implementation of IJobQueue that gives more priority to jobs that are submitted earlier.
 */
@NotThreadSafe
@ThreadSafetyGuaranteedBy("JobManager")
public class FIFOJobQueue implements IJobQueue {

    private static final Logger LOGGER = LogManager.getLogger();

    private final Map<JobId, JobRun> jobListMap = new LinkedHashMap<>();
    private final IJobManager jobManager;
    private final IJobCapacityController jobCapacityController;
    private final int jobQueueCapacity;

    public FIFOJobQueue(IJobManager jobManager, IJobCapacityController jobCapacityController) {
        this.jobManager = jobManager;
        this.jobCapacityController = jobCapacityController;
        this.jobQueueCapacity = jobManager.getJobQueueCapacity();
    }

    @Override
    public void add(JobRun run) throws HyracksException {
        int size = jobListMap.size();
        if (size >= jobQueueCapacity) {
            throw HyracksException.create(ErrorCode.JOB_QUEUE_FULL, jobQueueCapacity);
        }
        jobListMap.put(run.getJobId(), run);
    }

    @Override
    public JobRun remove(JobId jobId) {
        return jobListMap.remove(jobId);
    }

    @Override
    public JobRun get(JobId jobId) {
        return jobListMap.get(jobId);
    }

    @Override
    public List<JobRun> pull() {
        List<JobRun> jobRuns = new ArrayList<>();
        Iterator<JobRun> runIterator = jobListMap.values().iterator();
        while (runIterator.hasNext()) {
            JobRun run = runIterator.next();
            JobSpecification job = run.getJobSpecification();
            // Cluster maximum capacity can change over time, thus we have to re-check if the job should be rejected
            // or not.
            try {
                IJobCapacityController.JobSubmissionStatus status = jobCapacityController.allocate(job);
                // Checks if the job can be executed immediately.
                if (status == IJobCapacityController.JobSubmissionStatus.EXECUTE) {
                    jobRuns.add(run);
                    runIterator.remove(); // Removes the selected job.
                }
            } catch (HyracksException exception) {
                // The required capacity exceeds maximum capacity.
                List<Exception> exceptions = new ArrayList<>();
                exceptions.add(exception);
                runIterator.remove(); // Removes the job from the queue.
                try {
                    // Fails the job.
                    jobManager.prepareComplete(run, JobStatus.FAILURE_BEFORE_EXECUTION, exceptions);
                } catch (HyracksException e) {
                    LOGGER.log(Level.ERROR, e.getMessage(), e);
                }
            }
        }
        return jobRuns;
    }

    @Override
    public Collection<JobRun> jobs() {
        return Collections.unmodifiableCollection(jobListMap.values());
    }

    @Override
    public void clear() {
        jobListMap.clear();
    }
}
