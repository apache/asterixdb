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
package org.apache.hyracks.api.job;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksException;

/**
 * A listener for job related events
 */
public interface IJobLifecycleListener {
    /**
     * Notify the listener that a job has been created
     *
     * @param jobId
     * @param spec
     * @throws HyracksException
     */
    void notifyJobCreation(JobId jobId, JobSpecification spec) throws HyracksException;

    /**
     * Notify the listener that the job has started on the cluster controller
     *
     * @param jobId
     * @throws HyracksException
     */
    void notifyJobStart(JobId jobId) throws HyracksException;

    /**
     * Notify the listener that the job has been terminated, passing exceptions in case of failure
     *
     * @param jobId
     * @param jobStatus
     * @param exceptions
     * @throws HyracksException
     */
    void notifyJobFinish(JobId jobId, JobStatus jobStatus, List<Exception> exceptions) throws HyracksException;
}
