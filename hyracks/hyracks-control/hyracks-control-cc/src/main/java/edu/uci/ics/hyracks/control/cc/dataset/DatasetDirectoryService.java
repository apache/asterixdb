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
package edu.uci.ics.hyracks.control.cc.dataset;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.dataset.DatasetDirectoryRecord;
import edu.uci.ics.hyracks.api.dataset.IDatasetDirectoryService;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;

/**
 * TODO(madhusudancs): The potential perils of this global dataset directory service implementation is that, the jobs
 * location information is never evicted from the memory and the memory usage grows as the number of jobs in the system
 * grows. What we should possibly do is, add an API call for the client to say that it received everything it has to for
 * the job (after it receives all the results) completely. Then we can just get rid of the location information for that
 * job.
 */
public class DatasetDirectoryService implements IDatasetDirectoryService {
    private final Map<JobId, Map<ResultSetId, DatasetDirectoryRecord[]>> jobResultLocationsMap;

    public DatasetDirectoryService() {
        jobResultLocationsMap = new HashMap<JobId, Map<ResultSetId, DatasetDirectoryRecord[]>>();
    }

    @Override
    public synchronized void registerResultPartitionLocation(JobId jobId, ResultSetId rsId, int partition,
            int nPartitions, NetworkAddress networkAddress) {
        Map<ResultSetId, DatasetDirectoryRecord[]> resultSetsMap = jobResultLocationsMap.get(jobId);
        if (resultSetsMap == null) {
            resultSetsMap = new HashMap<ResultSetId, DatasetDirectoryRecord[]>();
            jobResultLocationsMap.put(jobId, resultSetsMap);
        }

        DatasetDirectoryRecord[] records = resultSetsMap.get(rsId);
        if (records == null) {
            records = new DatasetDirectoryRecord[nPartitions];
            resultSetsMap.put(rsId, records);
        }

        if (records[partition] == null) {
            records[partition] = new DatasetDirectoryRecord();
        }
        records[partition].setNetworkAddress(networkAddress);
        notifyAll();
    }

    @Override
    public synchronized DatasetDirectoryRecord[] getResultPartitionLocations(JobId jobId, ResultSetId rsId,
            DatasetDirectoryRecord[] knownRecords) throws HyracksDataException {
        while (!newRecords(jobId, rsId, knownRecords)) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }
        return jobResultLocationsMap.get(jobId).get(rsId);
    }

    private boolean newRecords(JobId jobId, ResultSetId rsId, DatasetDirectoryRecord[] knownRecords) {
        Map<ResultSetId, DatasetDirectoryRecord[]> rsMap = jobResultLocationsMap.get(jobId);
        if (rsMap == null) {
            return false;
        }
        DatasetDirectoryRecord[] records = rsMap.get(rsId);
        return !Arrays.equals(records, knownRecords);
    }
}