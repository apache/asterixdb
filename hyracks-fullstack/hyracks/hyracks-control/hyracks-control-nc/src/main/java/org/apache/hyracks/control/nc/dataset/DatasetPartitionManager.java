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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

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
import org.apache.hyracks.control.nc.io.WorkspaceFileFactory;
import org.apache.hyracks.control.nc.resources.DefaultDeallocatableRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DatasetPartitionManager implements IDatasetPartitionManager {
    private static final Logger LOGGER = LogManager.getLogger();

    private final NodeControllerService ncs;

    private final Executor executor;

    private final Map<JobId, ResultSetMap> partitionResultStateMap;

    private final DefaultDeallocatableRegistry deallocatableRegistry;

    private final IWorkspaceFileFactory fileFactory;

    private final DatasetMemoryManager datasetMemoryManager;

    public DatasetPartitionManager(NodeControllerService ncs, Executor executor, int availableMemory, long resultTTL,
            long resultSweepThreshold) {
        this.ncs = ncs;
        this.executor = executor;
        deallocatableRegistry = new DefaultDeallocatableRegistry();
        fileFactory = new WorkspaceFileFactory(deallocatableRegistry, ncs.getIoManager());
        if (availableMemory >= DatasetMemoryManager.getPageSize()) {
            datasetMemoryManager = new DatasetMemoryManager(availableMemory);
        } else {
            datasetMemoryManager = null;
        }
        partitionResultStateMap = new LinkedHashMap<>();
        executor.execute(new ResultStateSweeper(this, resultTTL, resultSweepThreshold, LOGGER));
    }

    @Override
    public IFrameWriter createDatasetPartitionWriter(IHyracksTaskContext ctx, ResultSetId rsId, boolean orderedResult,
            boolean asyncMode, int partition, int nPartitions) throws HyracksException {
        DatasetPartitionWriter dpw;
        JobId jobId = ctx.getJobletContext().getJobId();
        synchronized (this) {
            dpw = new DatasetPartitionWriter(ctx, this, jobId, rsId, asyncMode, orderedResult, partition, nPartitions,
                    datasetMemoryManager, fileFactory);

            ResultSetMap rsIdMap = partitionResultStateMap.computeIfAbsent(jobId, k -> new ResultSetMap());

            ResultState[] resultStates = rsIdMap.createOrGetResultStates(rsId, nPartitions);
            resultStates[partition] = dpw.getResultState();
        }

        LOGGER.debug("Initialized partition writer: JobId: " + jobId + ":partition: " + partition);
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
            LOGGER.debug("Reporting partition write completion: JobId: " + jobId + ": ResultSetId: " + rsId
                    + ":partition: " + partition);
            ncs.getClusterController().reportResultPartitionWriteCompletion(jobId, rsId, partition);
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    @Override
    public void initializeDatasetPartitionReader(JobId jobId, ResultSetId resultSetId, int partition,
            IFrameWriter writer) throws HyracksException {
        ResultState resultState = getResultState(jobId, resultSetId, partition);
        DatasetPartitionReader dpr = new DatasetPartitionReader(this, datasetMemoryManager, executor, resultState);
        dpr.writeTo(writer);
        LOGGER.debug("Initialized partition reader: JobId: " + jobId + ":ResultSetId: " + resultSetId + ":partition: "
                + partition);
    }

    protected synchronized ResultState getResultState(JobId jobId, ResultSetId resultSetId, int partition)
            throws HyracksException {
        ResultSetMap rsIdMap = partitionResultStateMap.get(jobId);
        if (rsIdMap == null) {
            throw new HyracksException("Unknown JobId " + jobId);
        }
        ResultState[] resultStates = rsIdMap.getResultStates(resultSetId);
        if (resultStates == null) {
            throw new HyracksException("Unknown JobId: " + jobId + " ResultSetId: " + resultSetId);
        }
        ResultState resultState = resultStates[partition];
        if (resultState == null) {
            throw new HyracksException("No DatasetPartitionWriter for partition " + partition);
        }
        return resultState;
    }

    @Override
    public synchronized void removePartition(JobId jobId, ResultSetId resultSetId, int partition) {
        ResultSetMap rsIdMap = partitionResultStateMap.get(jobId);
        if (rsIdMap != null && rsIdMap.removePartition(jobId, resultSetId, partition)) {
            partitionResultStateMap.remove(jobId);
        }
    }

    @Override
    public synchronized void abortReader(JobId jobId) {
        ResultSetMap rsIdMap = partitionResultStateMap.get(jobId);
        if (rsIdMap != null) {
            rsIdMap.abortAll();
        }
    }

    @Override
    public synchronized void abortAllReaders() {
        for (ResultSetMap rsIdMap : partitionResultStateMap.values()) {
            rsIdMap.abortAll();
        }
    }

    @Override
    public synchronized void close() {
        for (JobId jobId : getJobIds()) {
            deinit(jobId);
        }
        deallocatableRegistry.close();
    }

    @Override
    public synchronized Set<JobId> getJobIds() {
        return partitionResultStateMap.keySet();
    }

    @Override
    public ResultSetMap getState(JobId jobId) {
        return partitionResultStateMap.get(jobId);
    }

    @Override
    public synchronized long getResultTimestamp(JobId jobId) {
        IDatasetStateRecord r = getState(jobId);
        if (r == null) {
            return -1;
        }
        return r.getTimestamp();
    }

    @Override
    public synchronized void deinitState(JobId jobId) {
        deinit(jobId);
        partitionResultStateMap.remove(jobId);
    }

    private synchronized void deinit(JobId jobId) {
        ResultSetMap rsIdMap = partitionResultStateMap.get(jobId);
        if (rsIdMap != null) {
            rsIdMap.closeAndDeleteAll();
        }
    }

}
