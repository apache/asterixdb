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

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.job.JobId;

public class HyracksDataset implements IHyracksDataset {
    private final JobId jobId;
    private final IHyracksDatasetDirectoryServiceConnection datasetDirectoryServiceConnection;
    private NetworkAddress[] knownLocations;

    public HyracksDataset(JobId jobId, IHyracksDatasetDirectoryServiceConnection ddsc) {
        this.jobId = jobId;
        this.datasetDirectoryServiceConnection = ddsc;
        knownLocations = null;
    }

    private boolean nullExists(NetworkAddress[] locations) {
        if (locations == null) {
            return true;
        }
        for (int i = 0; i < locations.length; i++) {
            if (locations[i] == null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public ByteBuffer getResults() {
        while (nullExists(knownLocations)) {
            try {
                knownLocations = datasetDirectoryServiceConnection.getDatasetResultLocationsFunction(jobId,
                        knownLocations);
                if (knownLocations != null) {
                    System.out.println("knownLocations Length: " + knownLocations.length);
                    for (int i = 0; i < knownLocations.length; i++) {
                        System.out.println("knownLocations: " + knownLocations[i]);
                    }
                }
            } catch (Exception e) {
                // TODO(madhusudancs) Do something here
            }
        }

        return null;
    }

}
