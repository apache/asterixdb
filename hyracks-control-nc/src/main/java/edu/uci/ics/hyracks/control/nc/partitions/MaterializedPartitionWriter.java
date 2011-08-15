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
package edu.uci.ics.hyracks.control.nc.partitions;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileHandle;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.common.job.PartitionState;
import edu.uci.ics.hyracks.control.nc.io.IOManager;

public class MaterializedPartitionWriter implements IFrameWriter {
    protected final IHyracksRootContext ctx;

    protected final PartitionManager manager;

    protected final PartitionId pid;

    protected final TaskAttemptId taId;

    protected final Executor executor;

    private FileReference fRef;

    private FileHandle handle;

    private long size;

    public MaterializedPartitionWriter(IHyracksRootContext ctx, PartitionManager manager, PartitionId pid,
            TaskAttemptId taId, Executor executor) {
        this.ctx = ctx;
        this.manager = manager;
        this.pid = pid;
        this.taId = taId;
        this.executor = executor;
    }

    @Override
    public void open() throws HyracksDataException {
        fRef = manager.getFileFactory().createUnmanagedWorkspaceFile(pid.toString());
        handle = ctx.getIOManager().open(fRef, IIOManager.FileReadWriteMode.READ_WRITE,
                IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        size = 0;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        size += ctx.getIOManager().syncWrite(handle, size, buffer);
    }

    @Override
    public void flush() throws HyracksDataException {
    }

    @Override
    public void close() throws HyracksDataException {
        ctx.getIOManager().close(handle);
        manager.registerPartition(pid, taId,
                new MaterializedPartition(ctx, fRef, executor, (IOManager) ctx.getIOManager()),
                PartitionState.COMMITTED);
    }
}