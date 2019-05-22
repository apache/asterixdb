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
package org.apache.hyracks.control.nc.result;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.IWorkspaceFileFactory;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.partitions.ResultSetPartitionId;
import org.apache.hyracks.api.result.IResultMetadata;
import org.apache.hyracks.api.result.IResultPartitionManager;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResultPartitionWriter implements IFrameWriter {
    private static final Logger LOGGER = LogManager.getLogger();

    private final IResultPartitionManager manager;

    private final JobId jobId;

    private final ResultSetId resultSetId;

    private final IResultMetadata metadata;

    private final int partition;

    private final int nPartitions;

    private final ResultMemoryManager resultMemoryManager;

    private final ResultSetPartitionId resultSetPartitionId;

    private final ResultState resultState;

    private boolean partitionRegistered;

    private boolean failed = false;

    public ResultPartitionWriter(IHyracksTaskContext ctx, IResultPartitionManager manager, JobId jobId,
            ResultSetId rsId, boolean asyncMode, IResultMetadata metadata, int partition, int nPartitions,
            ResultMemoryManager resultMemoryManager, IWorkspaceFileFactory fileFactory, long maxReads) {
        this.manager = manager;
        this.jobId = jobId;
        this.resultSetId = rsId;
        this.metadata = metadata;
        this.partition = partition;
        this.nPartitions = nPartitions;
        this.resultMemoryManager = resultMemoryManager;

        resultSetPartitionId = new ResultSetPartitionId(jobId, rsId, partition);
        resultState = new ResultState(resultSetPartitionId, asyncMode, ctx.getIoManager(), fileFactory,
                ctx.getInitialFrameSize(), maxReads);
    }

    public ResultState getResultState() {
        return resultState;
    }

    @Override
    public void open() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("open(" + partition + ")");
        }
        partitionRegistered = false;
        resultState.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        registerResultPartitionLocation(false);
        if (resultMemoryManager == null) {
            resultState.write(buffer);
        } else {
            resultState.write(resultMemoryManager, buffer);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        failed = true;
        resultState.closeAndDelete();
        resultState.abort();
    }

    @Override
    public void close() throws HyracksDataException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("close(" + partition + ")");
        }
        try {
            if (!failed) {
                registerResultPartitionLocation(true);
            }
        } finally {
            resultState.close();
        }
        try {
            if (partitionRegistered) {
                manager.reportPartitionWriteCompletion(jobId, resultSetId, partition);
            }
        } catch (HyracksException e) {
            throw HyracksDataException.create(e);
        }
    }

    void registerResultPartitionLocation(boolean empty) throws HyracksDataException {
        try {
            if (!partitionRegistered) {
                manager.registerResultPartitionLocation(jobId, resultSetId, partition, nPartitions, metadata, empty);
                partitionRegistered = true;
            }
        } catch (HyracksException e) {
            throw HyracksDataException.create(e);
        }
    }
}
