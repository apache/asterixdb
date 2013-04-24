/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import java.util.List;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.dataset.DatasetJobRecord.Status;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IJobLifecycleListener;
import edu.uci.ics.hyracks.api.job.JobId;

public interface IDatasetDirectoryService extends IJobLifecycleListener {
    public void registerResultPartitionLocation(JobId jobId, ResultSetId rsId, boolean orderedResult, int partition,
            int nPartitions, NetworkAddress networkAddress);

    public void reportResultPartitionWriteCompletion(JobId jobId, ResultSetId rsId, int partition);

    public void reportResultPartitionFailure(JobId jobId, ResultSetId rsId, int partition);

    public void reportJobFailure(JobId jobId, List<Throwable> caughtExceptions);

    public Status getResultStatus(JobId jobId, ResultSetId rsId) throws HyracksDataException;

    public DatasetDirectoryRecord[] getResultPartitionLocations(JobId jobId, ResultSetId rsId,
            DatasetDirectoryRecord[] knownLocations) throws HyracksDataException;
}
