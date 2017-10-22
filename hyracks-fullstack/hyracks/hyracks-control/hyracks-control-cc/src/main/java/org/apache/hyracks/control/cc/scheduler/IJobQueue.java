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

import java.util.Collection;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.job.JobRun;

/**
 * This interface specifies a job queue.
 */
public interface IJobQueue {

    /**
     * Adds a job into the job queue.
     *
     * @param run,
     *            the descriptor of a job.
     * @throws HyracksException
     *             when the size of the queue exceeds its capacity.
     */
    void add(JobRun run) throws HyracksException;

    /**
     * Removes a job with a given jobId from the job queue.
     *
     * @param jobId,
     *            the job id of the job to be removed.
     */
    JobRun remove(JobId jobId);

    /**
     * Retrieves a job with a given jobId from the job queue.
     *
     * @param jobId,
     *            the job id of the job to be retrieved.
     */
    JobRun get(JobId jobId);

    /**
     * Pull a list of jobs from the job queque, when more cluster capacity becomes available.
     *
     * @return a list of jobs whose capacity requirements can all be met at the same time.
     */
    List<JobRun> pull();

    /**
     * @return all pending jobs in the queue.
     */
    Collection<JobRun> jobs();

    /**
     * Clears the job queue
     */
    void clear();
}
