/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.control.nc.dataset;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataset.IDatasetPartitionManager;
import org.apache.hyracks.api.dataset.IDatasetStateRecord;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.IWorkspaceFileFactory;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.common.dataset.ResultStateSweeper;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.control.nc.io.WorkspaceFileFactory;
import org.apache.hyracks.control.nc.resources.DefaultDeallocatableRegistry;

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
        synchronized (this) {
            dpw = new DatasetPartitionWriter(ctx, this, jobId, rsId, asyncMode, orderedResult, partition, nPartitions,
                    datasetMemoryManager, fileFactory);

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

        LOGGER.fine("Initialized partition writer: JobId: " + jobId + ":partition: " + partition);
        return dpw;
    }

    @Override
    public void registerResultPartitionLocation(JobId jobId, ResultSetId rsId, int partition, int nPartitions,
            boolean orderedResult, boolean emptyResult) throws HyracksException {
        try {
            // Be sure to send the *public* network address to the CC
            ncs.getClusterController().registerResultPartitionLocation(jobId, rsId, orderedResult, emptyResult,
                    partition, nPartitions, ncs.getDatasetNetworkManager().getPublicNetworkAddress());
        } catch (Exception e) {
            throw new HyracksException(e);
        }
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
            LOGGER.info("Reporting partition failure: JobId: " + jobId + " ResultSetId: " + rsId + " partition: "
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
    public Set<JobId> getJobIds() {
        return partitionResultStateMap.keySet();
    }

    @Override
    public IDatasetStateRecord getState(JobId jobId) {
        return partitionResultStateMap.get(jobId);
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
