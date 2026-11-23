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

import org.apache.hyracks.api.job.JobSpecification;

/**
 * Told each time a job leaves admission: when it starts executing, straight away or once enough capacity has been
 * released for it to leave the job queue, and when it leaves the queue without starting. Each job is reported at most
 * once; a job refused before it was queued (queue full, or more than the cluster's whole capacity) is not reported.
 */
@FunctionalInterface
public interface IJobAdmissionListener {

    /** How a job left admission. */
    enum Result {
        /** it started executing */
        ADMITTED,
        /** it was cancelled while queued, which is also how a client's timeout reaches a queued job */
        CANCELLED,
        /**
         * it was failed while queued: the cluster's capacity shrank below its requirement, or the cluster stopped
         * accepting jobs
         */
        REJECTED
    }

    IJobAdmissionListener NO_OP = (spec, waitNanos, result) -> {
    };

    /**
     * Called on the job manager's thread, so it must be quick and must not block.
     *
     * @param spec
     *            the job
     * @param waitNanos
     *            the nanoseconds from the job's creation to its leaving admission, including any time in the job queue
     * @param result
     *            how it left
     */
    void admissionEnded(JobSpecification spec, long waitNanos, Result result);
}
