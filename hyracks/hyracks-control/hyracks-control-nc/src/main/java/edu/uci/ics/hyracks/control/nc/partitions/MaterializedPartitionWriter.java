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
package edu.uci.ics.hyracks.control.nc.partitions;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IFileHandle;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.common.job.PartitionState;
import edu.uci.ics.hyracks.control.nc.io.IOManager;

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
        fRef = manager.getFileFactory().createUnmanagedWorkspaceFile(pid.toString());
        handle = ctx.getIOManager().open(fRef, IIOManager.FileReadWriteMode.READ_WRITE,
                IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        size = 0;
        failed = false;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
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
        ctx.getIOManager().close(handle);
        if (!failed) {
            manager.registerPartition(pid, taId,
                    new MaterializedPartition(ctx, fRef, executor, (IOManager) ctx.getIOManager()),
                    PartitionState.COMMITTED);
        }
    }
}