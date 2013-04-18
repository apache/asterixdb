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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataset.IDatasetPartitionManager;
import edu.uci.ics.hyracks.api.dataset.IDatasetPartitionReader;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.IWorkspaceFileFactory;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.control.nc.io.WorkspaceFileFactory;
import edu.uci.ics.hyracks.control.nc.resources.DefaultDeallocatableRegistry;

public class DatasetPartitionManager implements IDatasetPartitionManager {
    private static final Logger LOGGER = Logger.getLogger(DatasetPartitionManager.class.getName());

    private final NodeControllerService ncs;

    private final Executor executor;

    private final Map<JobId, ResultState[]> partitionResultStateMap;

    private final DefaultDeallocatableRegistry deallocatableRegistry;

    private final IWorkspaceFileFactory fileFactory;

    private final DatasetMemoryManager datasetMemoryManager;

    public DatasetPartitionManager(NodeControllerService ncs, Executor executor, int availableMemory,
            final int resultHistorySize) {
        this.ncs = ncs;
        this.executor = executor;
        deallocatableRegistry = new DefaultDeallocatableRegistry();
        fileFactory = new WorkspaceFileFactory(deallocatableRegistry, (IOManager) ncs.getRootContext().getIOManager());
        datasetMemoryManager = new DatasetMemoryManager(availableMemory);
        partitionResultStateMap = new LinkedHashMap<JobId, ResultState[]>() {
            private static final long serialVersionUID = 1L;

            protected boolean removeEldestEntry(Map.Entry<JobId, ResultState[]> eldest) {
                synchronized (DatasetPartitionManager.this) {
                    if (size() > resultHistorySize) {
                        ResultState[] resultStates = eldest.getValue();
                        for (int i = 0; i < resultStates.length; i++) {
                            ResultState state = resultStates[i];
                            if (state != null) {
                                state.deinit();
                                LOGGER.fine("Removing partition: " + i + " for JobId: " + eldest.getKey());
                            }
                        }
                        return true;
                    }
                    return false;
                }
            }
        };
    }

    @Override
    public IFrameWriter createDatasetPartitionWriter(IHyracksTaskContext ctx, ResultSetId rsId, boolean orderedResult,
            int partition, int nPartitions) throws HyracksException {
        DatasetPartitionWriter dpw = null;
        JobId jobId = ctx.getJobletContext().getJobId();
        try {
            synchronized (this) {
                ncs.getClusterController().registerResultPartitionLocation(jobId, rsId, orderedResult, partition,
                        nPartitions, ncs.getDatasetNetworkManager().getNetworkAddress());
                dpw = new DatasetPartitionWriter(ctx, this, jobId, rsId, partition, datasetMemoryManager);

                ResultState[] resultStates = partitionResultStateMap.get(jobId);
                if (resultStates == null) {
                    resultStates = new ResultState[nPartitions];
                    partitionResultStateMap.put(jobId, resultStates);
                }
                resultStates[partition] = dpw.getResultState();
            }
        } catch (Exception e) {
            throw new HyracksException(e);
        }

        LOGGER.fine("Initialized partition writer: JobId: " + jobId + ":partition: " + partition);
        return dpw;
    }

    @Override
    public void reportPartitionWriteCompletion(JobId jobId, ResultSetId rsId, int partition) throws HyracksException {
        try {
            LOGGER.fine("Reporting partition write completion: JobId: " + jobId + ": ResultSetId: " + rsId
                    + ":partition: " + partition);
            ncs.getClusterController().reportResultPartitionWriteCompletion(jobId, rsId, partition);
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    @Override
    public void reportPartitionFailure(JobId jobId, ResultSetId rsId, int partition) throws HyracksException {
        try {
            LOGGER.info("Reporting partition failure: JobId: " + jobId + ": ResultSetId: " + rsId + ":partition: "
                    + partition);
            ncs.getClusterController().reportResultPartitionFailure(jobId, rsId, partition);
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    @Override
    public void initializeDatasetPartitionReader(JobId jobId, int partition, IFrameWriter writer)
            throws HyracksException {
        ResultState resultState;
        synchronized (this) {
            ResultState[] resultStates = partitionResultStateMap.get(jobId);

            if (resultStates == null) {
                throw new HyracksException("Unknown JobId " + jobId);
            }

            resultState = resultStates[partition];
            if (resultState == null) {
                throw new HyracksException("No DatasetPartitionWriter for partition " + partition);
            }
        }

        IDatasetPartitionReader dpr = new DatasetPartitionReader(datasetMemoryManager, executor, resultState);
        dpr.writeTo(writer);
        LOGGER.fine("Initialized partition reader: JobId: " + jobId + ":partition: " + partition);
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
