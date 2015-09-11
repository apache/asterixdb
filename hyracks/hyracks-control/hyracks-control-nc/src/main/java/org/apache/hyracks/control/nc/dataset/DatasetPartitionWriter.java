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

import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataset.IDatasetPartitionManager;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.IWorkspaceFileFactory;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.partitions.ResultSetPartitionId;

public class DatasetPartitionWriter implements IFrameWriter {
    private static final Logger LOGGER = Logger.getLogger(DatasetPartitionWriter.class.getName());

    private final IDatasetPartitionManager manager;

    private final JobId jobId;

    private final ResultSetId resultSetId;

    private final boolean asyncMode;

    private final boolean orderedResult;

    private final int partition;

    private final int nPartitions;

    private final DatasetMemoryManager datasetMemoryManager;

    private final ResultSetPartitionId resultSetPartitionId;

    private final ResultState resultState;

    private boolean partitionRegistered;

    public DatasetPartitionWriter(IHyracksTaskContext ctx, IDatasetPartitionManager manager, JobId jobId,
            ResultSetId rsId, boolean asyncMode, boolean orderedResult, int partition, int nPartitions,
            DatasetMemoryManager datasetMemoryManager, IWorkspaceFileFactory fileFactory) {
        this.manager = manager;
        this.jobId = jobId;
        this.resultSetId = rsId;
        this.asyncMode = asyncMode;
        this.orderedResult = orderedResult;
        this.partition = partition;
        this.nPartitions = nPartitions;
        this.datasetMemoryManager = datasetMemoryManager;

        resultSetPartitionId = new ResultSetPartitionId(jobId, rsId, partition);
        resultState = new ResultState(resultSetPartitionId, asyncMode, ctx.getIOManager(), fileFactory,
                ctx.getInitialFrameSize());
    }

    public ResultState getResultState() {
        return resultState;
    }

    @Override
    public void open() {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("open(" + partition + ")");
        }
        partitionRegistered = false;
        resultState.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        if (!partitionRegistered) {
            registerResultPartitionLocation(false);
            partitionRegistered = true;
        }
        if (datasetMemoryManager == null) {
            resultState.write(buffer);
        } else {
            resultState.write(datasetMemoryManager, buffer);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        try {
            resultState.closeAndDelete();
            resultState.abort();
            manager.reportPartitionFailure(jobId, resultSetId, partition);
        } catch (HyracksException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("close(" + partition + ")");
        }
        if (!partitionRegistered) {
            registerResultPartitionLocation(true);
            partitionRegistered = true;
        }
        resultState.close();
        try {
            manager.reportPartitionWriteCompletion(jobId, resultSetId, partition);
        } catch (HyracksException e) {
            throw new HyracksDataException(e);
        }
    }

    void registerResultPartitionLocation(boolean empty) throws HyracksDataException {
        try {
            manager.registerResultPartitionLocation(jobId, resultSetId, partition, nPartitions, orderedResult, empty);
        } catch (HyracksException e) {
            if (e instanceof HyracksDataException) {
                throw (HyracksDataException) e;
            } else {
                throw new HyracksDataException(e);
            }
        }
    }

}
