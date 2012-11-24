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
import edu.uci.ics.hyracks.api.dataset.IDatasetDirectoryService;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;

/**
 * TODO: The potential perils of this global dataset directory service implementation is that, the jobs location information
 * is never evicted from the memory and the memory useage grows as the number of jobs in the system grows. What we should
 * possibly do is, add an API call for the client to say that it received everything it has to for the job (after it receives
 * all the results) completely. Then we can just get rid of the location information for that job.
 */
public class DatasetDirectoryService implements IDatasetDirectoryService {
    private final Map<JobId, NetworkAddress[]> jobPartitionLocationsMap;

    public DatasetDirectoryService() {
        jobPartitionLocationsMap = new HashMap<JobId, NetworkAddress[]>();
    }

    @Override
    public synchronized void registerResultPartitionLocation(JobId jobId, int partition, int nPartitions,
            NetworkAddress networkAddress) {
        NetworkAddress[] partitionLocations = jobPartitionLocationsMap.get(jobId);
        if (partitionLocations == null) {
            partitionLocations = new NetworkAddress[nPartitions];
            jobPartitionLocationsMap.put(jobId, partitionLocations);
        }

        partitionLocations[partition] = networkAddress;
        notifyAll();
    }

    @Override
    public synchronized NetworkAddress[] getResultPartitionLocations(JobId jobId, NetworkAddress[] knownLocations)
            throws HyracksDataException {
        while (Arrays.equals(jobPartitionLocationsMap.get(jobId), knownLocations)) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }
        return jobPartitionLocationsMap.get(jobId);
    }
}