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
import edu.uci.ics.hyracks.api.partitions.IPartition;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.common.job.PartitionState;
import edu.uci.ics.hyracks.control.nc.io.IOManager;

public class MaterializingPipelinedPartition implements IFrameWriter, IPartition {
    private static final Logger LOGGER = Logger.getLogger(MaterializingPipelinedPartition.class.getName());

    private final IHyracksTaskContext ctx;

    private final Executor executor;

    private final IOManager ioManager;

    private final PartitionManager manager;

    private final PartitionId pid;

    private final TaskAttemptId taId;

    private FileReference fRef;

    private IFileHandle handle;

    private long size;

    private boolean eos;

    private boolean failed;

    public MaterializingPipelinedPartition(IHyracksTaskContext ctx, PartitionManager manager, PartitionId pid,
            TaskAttemptId taId, Executor executor) {
        this.ctx = ctx;
        this.executor = executor;
        this.ioManager = (IOManager) ctx.getIOManager();
        this.manager = manager;
        this.pid = pid;
        this.taId = taId;
    }

    @Override
    public IHyracksTaskContext getTaskContext() {
        return ctx;
    }

    @Override
    public void deallocate() {
        fRef.delete();
    }

    @Override
    public void writeTo(final IFrameWriter writer) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    IFileHandle fh = ioManager.open(fRef, IIOManager.FileReadWriteMode.READ_ONLY,
                            IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
                    try {
                        writer.open();
                        try {
                            long offset = 0;
                            ByteBuffer buffer = ctx.allocateFrame();
                            boolean fail = false;
                            boolean done = false;
                            while (!fail && !done) {
                                synchronized (MaterializingPipelinedPartition.this) {
                                    while (offset >= size && !eos && !failed) {
                                        try {
                                            MaterializingPipelinedPartition.this.wait();
                                        } catch (InterruptedException e) {
                                            throw new HyracksDataException(e);
                                        }
                                    }
                                    fail = failed;
                                    done = eos && offset >= size;
                                }
                                if (fail) {
                                    writer.fail();
                                } else if (!done) {
                                    buffer.clear();
                                    long readLen = ioManager.syncRead(fh, offset, buffer);
                                    if (readLen < buffer.capacity()) {
                                        throw new HyracksDataException("Premature end of file");
                                    }
                                    offset += readLen;
                                    buffer.flip();
                                    writer.nextFrame(buffer);
                                }
                            }
                        } finally {
                            writer.close();
                        }
                    } finally {
                        ioManager.close(fh);
                    }
                } catch (HyracksDataException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Override
    public boolean isReusable() {
        return true;
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
        eos = false;
        failed = false;
        manager.registerPartition(pid, taId, this, PartitionState.STARTED);
    }

    @Override
    public synchronized void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        size += ctx.getIOManager().syncWrite(handle, size, buffer);
        notifyAll();
    }

    @Override
    public synchronized void fail() throws HyracksDataException {
        failed = true;
        notifyAll();
    }

    @Override
    public void close() throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("close(" + pid + " by " + taId);
        }
        boolean commit = false;
        synchronized (this) {
            eos = true;
            ctx.getIOManager().close(handle);
            handle = null;
            commit = !failed;
            notifyAll();
        }
        if (commit) {
            manager.updatePartitionState(pid, taId, this, PartitionState.COMMITTED);
        }
    }
}