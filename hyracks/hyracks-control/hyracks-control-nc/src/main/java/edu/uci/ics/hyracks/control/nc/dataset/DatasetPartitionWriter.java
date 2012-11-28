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
package edu.uci.ics.hyracks.control.nc.dataset;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataset.IDatasetPartitionManager;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IFileHandle;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.partitions.IPartition;
import edu.uci.ics.hyracks.comm.channels.NetworkOutputChannel;

public class DatasetPartitionWriter implements IFrameWriter, IPartition {
    private static final Logger LOGGER = Logger.getLogger(DatasetPartitionWriter.class.getName());

    private static final String FILE_PREFIX = "result_";

    private final IHyracksTaskContext ctx;

    private final IDatasetPartitionManager manager;

    private final int partition;

    private final Executor executor;

    private FileReference fRef;

    private IFileHandle handle;

    private long size;

    private boolean failed;

    public DatasetPartitionWriter(IHyracksTaskContext ctx, IDatasetPartitionManager manager, int partition,
            Executor executor) {
        this.ctx = ctx;
        this.manager = manager;
        this.partition = partition;
        this.executor = executor;
    }

    @Override
    public void open() throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("open(" + partition + ")");
        }
        fRef = manager.getFileFactory().createUnmanagedWorkspaceFile(FILE_PREFIX + String.valueOf(partition));
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
            LOGGER.info("close(" + partition + ")");
        }
        /* TODO(madhusudancs): Do something more intelligent here than closing the file handle because read still
         * wants it :-P
         */
        // ctx.getIOManager().close(handle);
    }

    @Override
    public IHyracksTaskContext getTaskContext() {
        return ctx;
    }

    private long read(long offset, ByteBuffer buffer) throws HyracksDataException {
        return ctx.getIOManager().syncRead(handle, offset, buffer);
    }

    @Override
    public void writeTo(final IFrameWriter writer) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                NetworkOutputChannel channel = (NetworkOutputChannel) writer;
                channel.setTaskContext(ctx);
                try {
                    channel.open();
                    try {
                        long offset = 0;
                        ByteBuffer buffer = ctx.allocateFrame();
                        while (true) {
                            buffer.clear();
                            long size = read(offset, buffer);
                            if (size < 0) {
                                break;
                            } else if (size < buffer.capacity()) {
                                throw new HyracksDataException("Premature end of file");
                            }
                            offset += size;
                            buffer.flip();
                            channel.nextFrame(buffer);
                        }
                    } finally {
                        channel.close();
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
    public void deallocate() {

    }
}
