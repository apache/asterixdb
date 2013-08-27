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
package edu.uci.ics.hyracks.control.nc.dataset;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataset.IDatasetPartitionManager;
import edu.uci.ics.hyracks.api.dataset.IDatasetStateRecord;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.IWorkspaceFileFactory;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.common.dataset.ResultStateSweeper;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.control.nc.io.WorkspaceFileFactory;
import edu.uci.ics.hyracks.control.nc.resources.DefaultDeallocatableRegistry;

public class DatasetPartitionManager implements IDatasetPartitionManager {
    private static final Logger LOGGER = Logger.getLogger(DatasetPartitionManager.class.getName());

    private final NodeControllerService ncs;

    private final Executor executor;

    private final Map<JobId, IDatasetStateRecord> partitionResultStateMap;

    private final DefaultDeallocatableRegistry deallocatableRegistry;

    private final IWorkspaceFileFactory fileFactory;

    private final DatasetMemoryManager datasetMemoryManager;

    public DatasetPartitionManager(NodeControllerService ncs, Executor executor, int availableMemory, long resultTTL,
            long resultSweepThreshold) {
        this.ncs = ncs;
        this.executor = executor;
        deallocatableRegistry = new DefaultDeallocatableRegistry();
        fileFactory = new WorkspaceFileFactory(deallocatableRegistry, (IOManager) ncs.getRootContext().getIOManager());
        if (availableMemory >= DatasetMemoryManager.getPageSize()) {
            datasetMemoryManager = new DatasetMemoryManager(availableMemory);
        } else {
            datasetMemoryManager = null;
        }
        partitionResultStateMap = new LinkedHashMap<JobId, IDatasetStateRecord>();
        executor.execute(new ResultStateSweeper(this, resultTTL, resultSweepThreshold));
    }

    @Override
    public IFrameWriter createDatasetPartitionWriter(IHyracksTaskContext ctx, ResultSetId rsId, boolean orderedResult,
            boolean asyncMode, int partition, int nPartitions) throws HyracksException {
        DatasetPartitionWriter dpw = null;
        JobId jobId = ctx.getJobletContext().getJobId();
        try {
            synchronized (this) {
                ncs.getClusterController().registerResultPartitionLocation(jobId, rsId, orderedResult, partition,
                        nPartitions, ncs.getDatasetNetworkManager().getNetworkAddress());
                dpw = new DatasetPartitionWriter(ctx, this, jobId, rsId, asyncMode, partition, datasetMemoryManager,
                        fileFactory);

                ResultSetMap rsIdMap = (ResultSetMap) partitionResultStateMap.get(jobId);
                if (rsIdMap == null) {
                    rsIdMap = new ResultSetMap();
                    partitionResultStateMap.put(jobId, rsIdMap);
                }

                ResultState[] resultStates = rsIdMap.get(rsId);
                if (resultStates == null) {
                    resultStates = new ResultState[nPartitions];
                    rsIdMap.put(rsId, resultStates);
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
    public void initializeDatasetPartitionReader(JobId jobId, ResultSetId resultSetId, int partition,
            IFrameWriter writer) throws HyracksException {
        ResultState resultState;
        synchronized (this) {
            ResultSetMap rsIdMap = (ResultSetMap) partitionResultStateMap.get(jobId);

            if (rsIdMap == null) {
                throw new HyracksException("Unknown JobId " + jobId);
            }

            ResultState[] resultStates = rsIdMap.get(resultSetId);
            if (resultStates == null) {
                throw new HyracksException("Unknown JobId: " + jobId + " ResultSetId: " + resultSetId);
            }

            resultState = resultStates[partition];
            if (resultState == null) {
                throw new HyracksException("No DatasetPartitionWriter for partition " + partition);
            }
        }

        DatasetPartitionReader dpr = new DatasetPartitionReader(this, datasetMemoryManager, executor, resultState);
        dpr.writeTo(writer);
        LOGGER.fine("Initialized partition reader: JobId: " + jobId + ":ResultSetId: " + resultSetId + ":partition: "
                + partition);
    }

    @Override
    public synchronized void removePartition(JobId jobId, ResultSetId resultSetId, int partition) {
        ResultSetMap rsIdMap = (ResultSetMap) partitionResultStateMap.get(jobId);
        if (rsIdMap != null) {
            ResultState[] resultStates = rsIdMap.get(resultSetId);
            if (resultStates != null) {
                ResultState state = resultStates[partition];
                if (state != null) {
                    state.closeAndDelete();
                    LOGGER.fine("Removing partition: " + partition + " for JobId: " + jobId);
                }
                resultStates[partition] = null;
                boolean stateEmpty = true;
                for (int i = 0; i < resultStates.length; i++) {
                    if (resultStates[i] != null) {
                        stateEmpty = false;
                        break;
                    }
                }
                if (stateEmpty) {
                    rsIdMap.remove(resultSetId);
                }
            }
            if (rsIdMap.isEmpty()) {
                partitionResultStateMap.remove(jobId);
            }
        }
    }

    @Override
    public synchronized void abortReader(JobId jobId) {
        ResultSetMap rsIdMap = (ResultSetMap) partitionResultStateMap.get(jobId);

        if (rsIdMap == null) {
            return;
        }

        for (ResultSetId rsId : rsIdMap.keySet()) {
            ResultState[] resultStates = rsIdMap.get(rsId);
            if (resultStates != null) {
                for (ResultState state : resultStates) {
                    if (state != null) {
                        state.abort();
                    }
                }
            }
        }
    }

    @Override
    public IWorkspaceFileFactory getFileFactory() {
        return fileFactory;
    }

    @Override
    public synchronized void close() {
        for (Entry<JobId, IDatasetStateRecord> entry : partitionResultStateMap.entrySet()) {
            deinit(entry.getKey());
        }
        deallocatableRegistry.close();
    }

    @Override
    public Map<JobId, IDatasetStateRecord> getStateMap() {
        return partitionResultStateMap;
    }

    @Override
    public void deinitState(JobId jobId) {
        deinit(jobId);
        partitionResultStateMap.remove(jobId);
    }

    private void deinit(JobId jobId) {
        ResultSetMap rsIdMap = (ResultSetMap) partitionResultStateMap.get(jobId);
        if (rsIdMap != null) {
            for (ResultSetId rsId : rsIdMap.keySet()) {
                ResultState[] resultStates = rsIdMap.get(rsId);
                if (resultStates != null) {
                    for (int i = 0; i < resultStates.length; i++) {
                        ResultState state = resultStates[i];
                        if (state != null) {
                            state.closeAndDelete();
                            LOGGER.fine("Removing partition: " + i + " for JobId: " + jobId);
                        }
                    }
                }
            }
        }
    }

    private class ResultSetMap extends HashMap<ResultSetId, ResultState[]> implements IDatasetStateRecord {
        private static final long serialVersionUID = 1L;

        long timestamp;

        public ResultSetMap() {
            super();
            timestamp = System.currentTimeMillis();
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
}
