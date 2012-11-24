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
package edu.uci.ics.hyracks.control.nc.dataset;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataset.IDatasetPartitionManager;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;

public class DatasetPartitionManager implements IDatasetPartitionManager {
    private final NodeControllerService ncs;

    private final Map<Pair<JobId, Integer>, IFrameWriter> partitionDatasetWriterMap;

    public DatasetPartitionManager(NodeControllerService ncs) {
        this.ncs = ncs;
        partitionDatasetWriterMap = new HashMap<Pair<JobId, Integer>, IFrameWriter>();
    }

    @Override
    public IFrameWriter createDatasetPartitionWriter(JobId jobId, int partition, int nPartitions)
            throws HyracksDataException {
        DatasetPartitionWriter dpw = null;
        try {
            ncs.getClusterController().registerResultPartitionLocation(jobId, partition, nPartitions,
                    ncs.getNetworkManager().getNetworkAddress());
            dpw = new DatasetPartitionWriter();
            partitionDatasetWriterMap.put(Pair.<JobId, Integer> of(jobId, partition), dpw);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }

        return dpw;
    }
}
