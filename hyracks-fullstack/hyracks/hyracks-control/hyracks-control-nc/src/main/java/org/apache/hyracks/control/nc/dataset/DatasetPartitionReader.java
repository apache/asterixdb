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
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.comm.channels.NetworkOutputChannel;

public class DatasetPartitionReader {
    private static final Logger LOGGER = Logger.getLogger(DatasetPartitionReader.class.getName());

    private final DatasetPartitionManager datasetPartitionManager;

    private final DatasetMemoryManager datasetMemoryManager;

    private final Executor executor;

    private final ResultState resultState;

    public DatasetPartitionReader(DatasetPartitionManager datasetPartitionManager,
            DatasetMemoryManager datasetMemoryManager, Executor executor, ResultState resultState) {
        this.datasetPartitionManager = datasetPartitionManager;
        this.datasetMemoryManager = datasetMemoryManager;
        this.executor = executor;
        this.resultState = resultState;
    }

    public void writeTo(final IFrameWriter writer) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                NetworkOutputChannel channel = (NetworkOutputChannel) writer;
                channel.setFrameSize(resultState.getFrameSize());
                try {
                    resultState.readOpen();
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
                        resultState.readClose();
                        // If the query is a synchronous query, remove its partition as soon as it is read.
                        if (!resultState.getAsyncMode()) {
                            datasetPartitionManager.removePartition(resultState.getResultSetPartitionId().getJobId(),
                                    resultState.getResultSetPartitionId().getResultSetId(), resultState
                                            .getResultSetPartitionId().getPartition());
                        }
                    }
                } catch (HyracksDataException e) {
                    throw new RuntimeException(e);
                }
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("result reading successful(" + resultState.getResultSetPartitionId() + ")");
                }
            }

            private long read(long offset, ByteBuffer buffer) throws HyracksDataException {
                if (datasetMemoryManager == null) {
                    return resultState.read(offset, buffer);
                } else {
                    return resultState.read(datasetMemoryManager, offset, buffer);
                }
            }
        });
    }
}
