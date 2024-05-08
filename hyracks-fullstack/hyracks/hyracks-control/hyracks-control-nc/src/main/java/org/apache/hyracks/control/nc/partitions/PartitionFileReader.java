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

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksCommonContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;

public class PartitionFileReader implements Runnable {

    private final IHyracksCommonContext ctx;
    private final FileReference partitionFile;
    private final IIOManager ioManager;
    private final IFrameWriter writer;
    private final boolean deleteFile;

    public PartitionFileReader(IHyracksCommonContext ctx, FileReference partitionFile, IIOManager ioManager,
            IFrameWriter writer, boolean deleteFile) {
        this.ctx = ctx;
        this.partitionFile = partitionFile;
        this.ioManager = ioManager;
        this.writer = writer;
        this.deleteFile = deleteFile;
    }

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
                try {
                    ioManager.close(fh);
                } finally {
                    if (deleteFile) {
                        FileUtils.deleteQuietly(partitionFile.getFile());
                    }
                }
            }
        } catch (HyracksDataException e) {
            throw new RuntimeException(e);
        }
    }
}
