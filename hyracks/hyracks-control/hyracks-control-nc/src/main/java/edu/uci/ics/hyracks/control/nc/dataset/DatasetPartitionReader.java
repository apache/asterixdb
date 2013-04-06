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
import edu.uci.ics.hyracks.api.dataset.IDatasetPartitionReader;
import edu.uci.ics.hyracks.api.dataset.Page;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IFileHandle;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.comm.channels.NetworkOutputChannel;

public class DatasetPartitionReader implements IDatasetPartitionReader {
    private static final Logger LOGGER = Logger.getLogger(DatasetPartitionReader.class.getName());

    private final DatasetMemoryManager datasetMemoryManager;

    private final Executor executor;

    private final ResultState resultState;

    private IFileHandle fileHandle;

    public DatasetPartitionReader(DatasetMemoryManager datasetMemoryManager, Executor executor, ResultState resultState) {
        this.datasetMemoryManager = datasetMemoryManager;
        this.executor = executor;
        this.resultState = resultState;
    }

    private long read(long offset, ByteBuffer buffer) throws HyracksDataException {
        long readSize = 0;
        synchronized (resultState) {
            while (offset >= resultState.getSize() && !resultState.getEOS()) {
                try {
                    resultState.wait();
                } catch (InterruptedException e) {
                    throw new HyracksDataException(e);
                }
            }
        }

        if (offset >= resultState.getSize() && resultState.getEOS()) {
            return readSize;
        }

        if (offset < resultState.getPersistentSize()) {
            readSize = resultState.getIOManager().syncRead(fileHandle, offset, buffer);
        }

        if (readSize < buffer.capacity()) {
            long localPageOffset = offset - resultState.getPersistentSize();
            int localPageIndex = (int) (localPageOffset / datasetMemoryManager.getPageSize());
            int pageOffset = (int) (localPageOffset % datasetMemoryManager.getPageSize());
            Page page = resultState.getPage(localPageIndex);
            if (page == null) {
            	return readSize;
            }
            readSize += buffer.remaining();
            buffer.put(page.getBuffer().array(), pageOffset, buffer.remaining());
        }

        datasetMemoryManager.pageReferenced(resultState.getResultSetPartitionId());
        return readSize;
    }

    @Override
    public void writeTo(final IFrameWriter writer) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                NetworkOutputChannel channel = (NetworkOutputChannel) writer;
                channel.setFrameSize(resultState.getFrameSize());
                try {
                    fileHandle = resultState.getIOManager().open(resultState.getValidFileReference(),
                            IIOManager.FileReadWriteMode.READ_ONLY, IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
                    channel.open();
                    try {
                        long offset = 0;
                        ByteBuffer buffer = ByteBuffer.allocate(resultState.getFrameSize());
                        while (true) {
                            buffer.clear();
                            long size = read(offset, buffer);
                            if (size <= 0) {
                                break;
                            } else if (size < buffer.limit()) {
                                throw new HyracksDataException("Premature end of file - readSize: " + size
                                        + " buffer limit: " + buffer.limit());
                            }
                            offset += size;
                            buffer.flip();
                            channel.nextFrame(buffer);
                        }
                    } finally {
                        channel.close();
                        resultState.getIOManager().close(fileHandle);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (HyracksDataException e) {
                    throw new RuntimeException(e);
                }
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("result reading successful(" + resultState.getResultSetPartitionId() + ")");
                }
            }
        });
    }
}
