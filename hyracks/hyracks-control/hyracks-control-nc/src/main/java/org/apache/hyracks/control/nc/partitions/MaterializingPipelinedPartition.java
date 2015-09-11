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
import org.apache.hyracks.api.partitions.IPartition;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.control.common.job.PartitionState;
import org.apache.hyracks.control.nc.io.IOManager;

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
        if (fRef != null) {
            fRef.delete();
        }
    }

    @Override
    public void writeTo(final IFrameWriter writer) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    synchronized (MaterializingPipelinedPartition.this) {
                        while (fRef == null && eos == false) {
                            MaterializingPipelinedPartition.this.wait();
                        }
                    }
                    IFileHandle fh = fRef == null ? null : ioManager.open(fRef,
                            IIOManager.FileReadWriteMode.READ_ONLY, IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
                    try {
                        writer.open();
                        try {
                            if (fh != null) {
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
                            }
                        } finally {
                            writer.close();
                        }
                    } finally {
                        if (fh != null) {
                            ioManager.close(fh);
                        }
                    }
                } catch (Exception e) {
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
        size = 0;
        eos = false;
        failed = false;
        manager.registerPartition(pid, taId, this, PartitionState.STARTED, false);
    }

    private void checkOrCreateFile() throws HyracksDataException {
        if (fRef == null) {
            fRef = manager.getFileFactory().createUnmanagedWorkspaceFile(pid.toString().replace(":", "$"));
            handle = ctx.getIOManager().open(fRef, IIOManager.FileReadWriteMode.READ_WRITE,
                    IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        }
    }

    @Override
    public synchronized void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        checkOrCreateFile();
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
        synchronized (this) {
            eos = true;
            if (handle != null) {
                ctx.getIOManager().close(handle);
            }
            handle = null;
            notifyAll();
        }
    }
}