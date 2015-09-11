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
package org.apache.hyracks.control.nc.partitions;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.control.common.job.PartitionState;
import org.apache.hyracks.control.nc.io.IOManager;

public class MaterializedPartitionWriter implements IFrameWriter {
    private static final Logger LOGGER = Logger.getLogger(MaterializedPartitionWriter.class.getName());

    private final IHyracksTaskContext ctx;

    private final PartitionManager manager;

    private final PartitionId pid;

    private final TaskAttemptId taId;

    private final Executor executor;

    private FileReference fRef;

    private IFileHandle handle;

    private long size;

    private boolean failed;

    public MaterializedPartitionWriter(IHyracksTaskContext ctx, PartitionManager manager, PartitionId pid,
            TaskAttemptId taId, Executor executor) {
        this.ctx = ctx;
        this.manager = manager;
        this.pid = pid;
        this.taId = taId;
        this.executor = executor;
    }

    @Override
    public void open() throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("open(" + pid + " by " + taId);
        }
        failed = false;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        if (handle == null) {
            fRef = manager.getFileFactory().createUnmanagedWorkspaceFile(pid.toString());
            handle = ctx.getIOManager().open(fRef, IIOManager.FileReadWriteMode.READ_WRITE,
                    IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
            size = 0;
        }
        size += ctx.getIOManager().syncWrite(handle, size, buffer);
    }

    @Override
    public void fail() throws HyracksDataException {
        failed = true;
    }

    @Override
    public void close() throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("close(" + pid + " by " + taId);
        }
        if (handle != null) {
            ctx.getIOManager().close(handle);
        }
        if (!failed) {
            manager.registerPartition(pid, taId,
                    new MaterializedPartition(ctx, fRef, executor, (IOManager) ctx.getIOManager()),
                    PartitionState.COMMITTED, taId.getAttempt() == 0 ? false : true);

        }
    }
}