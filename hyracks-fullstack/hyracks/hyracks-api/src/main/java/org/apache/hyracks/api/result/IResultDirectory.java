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
package org.apache.hyracks.api.result;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.ResultJobRecord.Status;

public interface IResultDirectory {
    /**
     * Gets the result status for the given result set.
     *
     * @param jobId
     *            ID of the job
     * @param rsId
     *            ID of the result set
     * @return {@link Status}
     * @throws Exception
     */
    Status getResultStatus(JobId jobId, ResultSetId rsId) throws Exception;

    /**
     * Gets the IP Addresses and ports for the partition generating the result for each location.
     *
     * @param jobId
     *            ID of the job
     * @param rsId
     *            ID of the result set
     * @param knownRecords
     *            Locations that are already known to the client
     * @return {@link ResultDirectoryRecord[]}
     * @throws Exception
     */
    ResultDirectoryRecord[] getResultLocations(JobId jobId, ResultSetId rsId, ResultDirectoryRecord[] knownRecords)
            throws Exception;

    /**
     * Gets the result metadata
     * @param jobId
     * @param rsId
     * @return
     * @throws Exception
     */
    IResultMetadata getResultMetadata(JobId jobId, ResultSetId rsId) throws Exception;
}
