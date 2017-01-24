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

package org.apache.hyracks.api.job.resource;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobSpecification;

/**
 * This interface determines the behavior of a job when it is submitted to the job manager.
 * The job could be one of the following three cases:
 * -- rejected immediately because its capacity requirement exceeds the cluster's capacity.
 * -- entered into a pending job queue for deferred execution, due to the current capacity limitation because of
 * concurrent running jobs;
 * -- executed immediately because there is sufficient capacity.
 */
public interface IJobCapacityController {

    enum JobSubmissionStatus {
        EXECUTE,
        QUEUE
    }

    /**
     * Allocates required cluster capacity for a job.
     *
     * @param job,
     *            the job specification.
     * @return EXECUTE, if the job can be executed immediately;
     *         QUEUE, if the job cannot be executed
     * @throws HyracksException
     *             if the job's capacity requirement exceeds the maximum capacity of the cluster.
     */
    JobSubmissionStatus allocate(JobSpecification job) throws HyracksException;

    /**
     * Releases cluster capacity for a job when it completes.
     *
     * @param job,
     *            the job specification.
     */
    void release(JobSpecification job);

}
