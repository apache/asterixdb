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

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IFileHandle;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.partitions.IPartition;
import edu.uci.ics.hyracks.control.nc.io.IOManager;

public class MaterializedPartition implements IPartition {
    private final IHyracksTaskContext ctx;

    private final FileReference partitionFile;

    private final Executor executor;

    private final IOManager ioManager;

    public MaterializedPartition(IHyracksTaskContext ctx, FileReference partitionFile, Executor executor,
            IOManager ioManager) {
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
        partitionFile.delete();
    }

    @Override
    public void writeTo(final IFrameWriter writer) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
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