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
package org.apache.hyracks.control.nc.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;

public class FileHandle implements IFileHandle {

    private final FileReference fileRef;
    private RandomAccessFile raf;
    private String mode;

    public FileHandle(FileReference fileRef) {
        this.fileRef = fileRef;
    }

    /**
     * Open the file
     *
     * @param rwMode
     * @param syncMode
     * @throws IOException
     */
    public void open(IIOManager.FileReadWriteMode rwMode, IIOManager.FileSyncMode syncMode) throws IOException {
        if (!fileRef.getFile().exists()) {
            throw HyracksDataException.create(ErrorCode.FILE_DOES_NOT_EXIST, fileRef.getAbsolutePath());
        }
        switch (rwMode) {
            case READ_ONLY:
                mode = "r";
                break;
            case READ_WRITE:
                switch (syncMode) {
                    case METADATA_ASYNC_DATA_ASYNC:
                        mode = "rw";
                        break;
                    case METADATA_ASYNC_DATA_SYNC:
                        mode = "rwd";
                        break;
                    case METADATA_SYNC_DATA_SYNC:
                        mode = "rws";
                        break;
                    default:
                        throw new IllegalArgumentException();
                }
                break;

            default:
                throw new IllegalArgumentException();
        }
        ensureOpen();
    }

    public void close() throws IOException {
        if (raf == null) {
            return;
        }
        raf.close();
        raf = null;
    }

    @Override
    public FileReference getFileReference() {
        return fileRef;
    }

    public FileChannel getFileChannel() {
        return raf.getChannel();
    }

    public synchronized void ensureOpen() throws HyracksDataException {
        if (raf == null || !raf.getChannel().isOpen()) {
            try {
                raf = new RandomAccessFile(fileRef.getFile(), mode);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
    }
}
