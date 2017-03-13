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

package org.apache.asterix.translator;

import org.apache.hyracks.api.job.JobId;

/**
 * The context for statement executors, which maintains the meta information of all queries.
 * TODO(yingyi): also maintain the mapping from server generated request ids to jobs.
 */
public interface IStatementExecutorContext {

    /**
     * Gets the Hyracks JobId from the user-provided client context id.
     *
     * @param clientContextId,
     *            a user provided client context id.
     * @return the Hyracks job id of class {@link org.apache.hyracks.api.job.JobId}.
     */
    JobId getJobIdFromClientContextId(String clientContextId);

    /**
     * Puts a client context id for a statement and the corresponding Hyracks job id.
     *
     * @param clientContextId,
     *            a user provided client context id.
     * @param jobId,
     *            the Hyracks job id of class {@link org.apache.hyracks.api.job.JobId}.
     */
    void put(String clientContextId, JobId jobId);

    /**
     * Removes the information about the query corresponding to a user-provided client context id.
     *
     * @param clientContextId,
     *            a user provided client context id.
     */
    JobId removeJobIdFromClientContextId(String clientContextId);
}
