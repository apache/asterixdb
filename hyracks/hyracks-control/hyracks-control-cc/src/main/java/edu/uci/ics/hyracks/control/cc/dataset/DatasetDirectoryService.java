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

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.dataset.IDatasetDirectoryService;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class DatasetDirectoryService implements IDatasetDirectoryService {
    private NetworkAddress[] partitionLocations;

    public DatasetDirectoryService() {
        partitionLocations = null;
    }

    @Override
    public synchronized void registerResultPartitionLocation(int partition, int nPartitions,
            NetworkAddress networkAddress) {
        if (partitionLocations == null) {
            partitionLocations = new NetworkAddress[nPartitions];
        }

        partitionLocations[partition] = networkAddress;
        notifyAll();
    }

    @Override
    public synchronized NetworkAddress[] getResultPartitionLocations(NetworkAddress[] knownLocations)
            throws HyracksDataException {
        while (Arrays.equals(partitionLocations, knownLocations)) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }
        return partitionLocations;
    }
}