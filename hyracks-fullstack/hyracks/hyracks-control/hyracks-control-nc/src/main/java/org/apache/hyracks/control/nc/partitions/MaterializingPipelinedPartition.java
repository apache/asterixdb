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

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.partitions.IPartition;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.control.common.job.PartitionState;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MaterializingPipelinedPartition implements IFrameWriter, IPartition {
    private static final Logger LOGGER = LogManager.getLogger();

    private final IHyracksTaskContext ctx;
    private final Executor executor;
    private final IIOManager ioManager;
    private final PartitionManager manager;
    private final PartitionId pid;
    private final TaskAttemptId taId;
    private FileReference fRef;
    private IFileHandle writeHandle;
    private long size;
    private boolean eos;
    private boolean failed;
    protected boolean flushRequest;
    private boolean deallocated;
    private Level openCloseLevel = Level.DEBUG;
    private Thread dataConsumerThread;

    public MaterializingPipelinedPartition(IHyracksTaskContext ctx, PartitionManager manager, PartitionId pid,
            TaskAttemptId taId, Executor executor) {
        this.ctx = ctx;
        this.executor = executor;
        this.ioManager = ctx.getIoManager();
        this.manager = manager;
        this.pid = pid;
        this.taId = taId;
    }

    @Override
    public IHyracksTaskContext getTaskContext() {
        return ctx;
    }

    @Override
    public synchronized void deallocate() {
        // Makes sure that the data consumer thread will not wait for anything further. Since the receiver side could
        // have be interrupted already, the data consumer thread can potentially hang on writer.nextFrame(...)
        // or writer.close(...).  Note that Task.abort(...) cannot interrupt the dataConsumerThread.
        // If the query runs successfully, the dataConsumer thread should have been completed by this time.
        if (dataConsumerThread != null) {
            dataConsumerThread.interrupt();
        }
        deallocated = true;
    }

    @Override
    public void writeTo(final IFrameWriter writer) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                Thread thread = Thread.currentThread();
                setDataConsumerThread(thread); // Sets the data consumer thread to the current thread.
                try {
                    thread.setName(MaterializingPipelinedPartition.this.getClass().getSimpleName() + " " + pid);
                    FileReference fRefCopy;
                    synchronized (MaterializingPipelinedPartition.this) {
                        while (fRef == null && !eos && !failed) {
                            MaterializingPipelinedPartition.this.wait();
                        }
                        fRefCopy = fRef;
                    }
                    writer.open();
                    IFileHandle readHandle = fRefCopy == null ? null
                            : ioManager.open(fRefCopy, IIOManager.FileReadWriteMode.READ_ONLY,
                                    IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
                    try {
                        if (readHandle == null) {
                            // Either fail() is called or close() is called with 0 tuples coming in.
                            return;
                        }
                        synchronized (MaterializingPipelinedPartition.this) {
                            if (deallocated) {
                                return;
                            }
                        }
                        long offset = 0;
                        ByteBuffer buffer = ctx.allocateFrame();
                        boolean done = false;
                        while (!done) {
                            boolean flush;
                            boolean fail;
                            synchronized (MaterializingPipelinedPartition.this) {
                                while (offset >= size && !eos && !failed) {
                                    MaterializingPipelinedPartition.this.wait();
                                }
                                flush = flushRequest;
                                flushRequest = false; // Clears the flush flag.
                                fail = failed;
                                done = eos && offset >= size;
                            }
                            if (fail) {
                                writer.fail(); // Exits the loop and the try-block if fail() is called.
                                break;
                            }
                            if (!done) {
                                buffer.clear();
                                long readLen = ioManager.syncRead(readHandle, offset, buffer);
                                if (readLen < buffer.capacity()) {
                                    throw new HyracksDataException("Premature end of file");
                                }
                                offset += readLen;
                                buffer.flip();
                                writer.nextFrame(buffer);
                            }
                            if (flush) {
                                writer.flush(); // Flushes the writer if flush() is called.
                            }
                        }
                    } catch (Exception e) {
                        writer.fail();
                        throw e;
                    } finally {
                        try {
                            writer.close();
                        } finally {
                            // Makes sure that the reader is always closed and the temp file is always deleted.
                            try {
                                if (readHandle != null) {
                                    ioManager.close(readHandle);
                                }
                            } finally {
                                if (fRef != null) {
                                    fRef.delete();
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    LOGGER.log(ExceptionUtils.causedByInterrupt(e) ? Level.DEBUG : Level.WARN,
                            "Failure writing to a frame", e);
                } finally {
                    setDataConsumerThread(null); // Sets back the data consumer thread to null.
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
        if (LOGGER.isEnabled(openCloseLevel)) {
            LOGGER.log(openCloseLevel, "open(" + pid + " by " + taId);
        }
        size = 0;
        eos = false;
        failed = false;
        deallocated = false;
        manager.registerPartition(pid, ctx.getJobletContext().getJobId().getCcId(), taId, this, PartitionState.STARTED,
                false);
    }

    private void checkOrCreateFile() throws HyracksDataException {
        if (fRef == null) {
            fRef = manager.getFileFactory().createUnmanagedWorkspaceFile(pid.toString().replace(":", "$"));
            writeHandle = ioManager.open(fRef, IIOManager.FileReadWriteMode.READ_WRITE,
                    IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        }
    }

    @Override
    public synchronized void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        checkOrCreateFile();
        size += ctx.getIoManager().syncWrite(writeHandle, size, buffer);
        notifyAll();
    }

    @Override
    public synchronized void fail() throws HyracksDataException {
        failed = true;
        notifyAll();
    }

    @Override
    public void close() throws HyracksDataException {
        if (LOGGER.isEnabled(openCloseLevel)) {
            LOGGER.log(openCloseLevel, "close(" + pid + " by " + taId);
        }
        if (writeHandle != null) {
            ctx.getIoManager().close(writeHandle);
        }
        synchronized (this) {
            eos = true;
            writeHandle = null;
            notifyAll();
        }
    }

    @Override
    public synchronized void flush() throws HyracksDataException {
        flushRequest = true;
        notifyAll();
    }

    // Sets the data consumer thread.
    private synchronized void setDataConsumerThread(Thread thread) {
        dataConsumerThread = thread;
    }

}
