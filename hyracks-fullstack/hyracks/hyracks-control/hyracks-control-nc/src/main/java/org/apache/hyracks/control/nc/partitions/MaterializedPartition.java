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
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.partitions.IPartition;

public class MaterializedPartition implements IPartition {
    private final IHyracksTaskContext ctx;

    private final FileReference partitionFile;

    private final Executor executor;

    private final IIOManager ioManager;

    public MaterializedPartition(IHyracksTaskContext ctx, FileReference partitionFile, Executor executor,
            IIOManager ioManager) {
        this.ctx = ctx;
        this.partitionFile = partitionFile;
        this.executor = executor;
        this.ioManager = ioManager;
    }

    @Override
    public IHyracksTaskContext getTaskContext() {
        return ctx;
    }

    @Override
    public void deallocate() {
        if (partitionFile != null) {
            partitionFile.delete();
        }
    }

    @Override
    public void writeTo(final IFrameWriter writer) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    if (partitionFile == null) {
                        writer.open();
                        writer.close();
                        return;
                    }
                    IFileHandle fh = ioManager.open(partitionFile, IIOManager.FileReadWriteMode.READ_ONLY,
                            IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
                    try {
                        writer.open();
                        try {
                            long offset = 0;
                            ByteBuffer buffer = ctx.allocateFrame();
                            while (true) {
                                buffer.clear();
                                long size = ioManager.syncRead(fh, offset, buffer);
                                if (size < 0) {
                                    break;
                                } else if (size < buffer.capacity()) {
                                    throw new HyracksDataException("Premature end of file");
                                }
                                offset += size;
                                buffer.flip();
                                writer.nextFrame(buffer);
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
}
