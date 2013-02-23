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
import java.util.concurrent.Executor;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataset.IDatasetPartitionManager;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.IWorkspaceFileFactory;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.control.nc.io.WorkspaceFileFactory;
import edu.uci.ics.hyracks.control.nc.resources.DefaultDeallocatableRegistry;

public class DatasetPartitionManager implements IDatasetPartitionManager {
    private final NodeControllerService ncs;

    private final Executor executor;

    private final Map<JobId, DatasetPartitionWriter[]> partitionDatasetWriterMap;

    private final DefaultDeallocatableRegistry deallocatableRegistry;

    private final IWorkspaceFileFactory fileFactory;

    public DatasetPartitionManager(NodeControllerService ncs, Executor executor) {
        this.ncs = ncs;
        this.executor = executor;
        partitionDatasetWriterMap = new HashMap<JobId, DatasetPartitionWriter[]>();
        deallocatableRegistry = new DefaultDeallocatableRegistry();
        fileFactory = new WorkspaceFileFactory(deallocatableRegistry, (IOManager) ncs.getRootContext().getIOManager());
    }

    @Override
    public IFrameWriter createDatasetPartitionWriter(IHyracksTaskContext ctx, ResultSetId rsId, boolean orderedResult,
            int partition, int nPartitions) throws HyracksException {
        DatasetPartitionWriter dpw = null;
        JobId jobId = ctx.getJobletContext().getJobId();
        try {
            ncs.getClusterController().registerResultPartitionLocation(jobId, rsId, orderedResult, partition,
                    nPartitions, ncs.getDatasetNetworkManager().getNetworkAddress());
            dpw = new DatasetPartitionWriter(ctx, this, jobId, rsId, partition, executor);

            DatasetPartitionWriter[] writers = partitionDatasetWriterMap.get(jobId);
            if (writers == null) {
                writers = new DatasetPartitionWriter[nPartitions];
                partitionDatasetWriterMap.put(jobId, writers);
            }
            writers[partition] = dpw;
        } catch (Exception e) {
            throw new HyracksException(e);
        }

        return dpw;
    }

    @Override
    public void reportPartitionWriteCompletion(JobId jobId, ResultSetId rsId, int partition) throws HyracksException {
        try {
            ncs.getClusterController().reportResultPartitionWriteCompletion(jobId, rsId, partition);
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    @Override
    public void reportPartitionFailure(JobId jobId, ResultSetId rsId, int partition) throws HyracksException {
        try {
            ncs.getClusterController().reportResultPartitionFailure(jobId, rsId, partition);
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    @Override
    public void initializeDatasetPartitionReader(JobId jobId, int partition, IFrameWriter writer)
            throws HyracksException {
        DatasetPartitionWriter[] writers = partitionDatasetWriterMap.get(jobId);
        if (writers == null) {
            throw new HyracksException("Unknown JobId " + jobId);
        }

        DatasetPartitionWriter dpw = writers[partition];
        if (dpw == null) {
            throw new HyracksException("No DatasetPartitionWriter for partition " + partition);
        }

        dpw.writeTo(writer);
    }

    @Override
    public IWorkspaceFileFactory getFileFactory() {
        return fileFactory;
    }

    @Override
    public void close() {
        deallocatableRegistry.close();
    }
}
