/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.api.dataset;

import edu.uci.ics.hyracks.api.dataset.DatasetJobRecord.Status;
import edu.uci.ics.hyracks.api.job.JobId;

public interface IHyracksDatasetDirectoryServiceInterface {
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
    public Status getDatasetResultStatus(JobId jobId, ResultSetId rsId) throws Exception;

    /**
     * Gets the IP Addresses and ports for the partition generating the result for each location.
     * 
     * @param jobId
     *            ID of the job
     * @param rsId
     *            ID of the result set
     * @param knownRecords
     *            Locations from the dataset directory that are already known to the client
     * @return {@link NetworkAddress[]}
     * @throws Exception
     */
    public DatasetDirectoryRecord[] getDatasetResultLocations(JobId jobId, ResultSetId rsId,
            DatasetDirectoryRecord[] knownRecords) throws Exception;
}
