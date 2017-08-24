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

package org.apache.hyracks.control.cc.job;

import java.util.Collection;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.control.common.work.IResultCallback;

/**
 * This interface abstracts the job lifecycle management and job scheduling for a cluster.
 */
public interface IJobManager {

    /**
     * Enters a new job into the job manager.
     * It's up to the implementation to decide whether to execute the job immediately, queue it for the
     * later execution, or reject it immediately..
     *
     * @param jobRun,
     *            the descriptor of a job.
     * @throws HyracksException
     *             when the job cannot even be accepted by the job manager, for example, when the pending
     *             job queue is too large or the capacity requirement exceeds the capacity of the cluster.
     */
    void add(JobRun jobRun) throws HyracksException;

    /**
     * Cancel a job with a given job id.
     *
     * @param callback
     *
     * @param jobId,
     *            the id of the job.
     */
    void cancel(JobId jobId, IResultCallback<Void> callback) throws HyracksException;

    /**
     * This method is called when the master process decides to complete job.
     * The implementation of this method should instruct all involved worker processes to clean the state of each
     * individual parallel partition up.
     *
     * If there is no involved worker processes, the method is responsible to call
     * <code>finalComplete</code> directly, for example, when all worker processes died during the job execution.
     *
     * @param jobRun,
     *            the descriptor of a job.
     * @param status,
     *            the final status of the job.
     * @param exceptions,
     *            a list of exceptions that are caught during job execution.
     * @throws HyracksException
     *             if anything goes wrong during the final job completion. No partial job states should be left.
     */
    void prepareComplete(JobRun jobRun, JobStatus status, List<Exception> exceptions) throws HyracksException;

    /**
     * This method gets called when all worker processes have notified the master that their individual parallel
     * partition is completed and their corresponding states are cleaned up.
     * The implementation of this method only needs to cleanup the states of a job within the master
     * process.
     *
     * @param jobRun,
     *            the descriptor of a job.
     * @throws HyracksException
     *             if anything goes wrong during the final job completion. No partial job states should be left.
     */
    void finalComplete(JobRun jobRun) throws HyracksException;

    /**
     * Retrieves a job from a given job id.
     *
     * @param jobId,
     *            the id of the job.
     * @return the job run, which is the descriptor a the job, or null if the job cannot be found.
     */
    JobRun get(JobId jobId);

    /**
     * Retrieves the exception records for a historical job.
     *
     * @param jobId,
     *            the job id.
     * @return the exception records of a job that has been removed from the archive but still stays in the stored
     *         history, where each historical job is paired with exceptions during its execution.
     */
    List<Exception> getExceptionHistory(JobId jobId);

    /**
     * @return all jobs that are currently running.
     */
    Collection<JobRun> getRunningJobs();

    /**
     * @return all jobs that are currently waiting in the job queue.
     */
    Collection<JobRun> getPendingJobs();

    /**
     * @return all jobs that are completed or terminated, but not yet discarded.
     */
    Collection<JobRun> getArchivedJobs();

    /**
     * @return the maximum number of jobs to queue before rejecting new jobs
     */
    int getJobQueueCapacity();
}
