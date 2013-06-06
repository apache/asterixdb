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
package edu.uci.ics.hyracks.control.nc.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IFileHandle;
import edu.uci.ics.hyracks.api.io.IIOManager;

public class FileHandle implements IFileHandle {
    private final FileReference fileRef;

    private RandomAccessFile raf;

    private FileChannel channel;

    public FileHandle(FileReference fileRef) {
        this.fileRef = fileRef;
    }

    public void open(IIOManager.FileReadWriteMode rwMode, IIOManager.FileSyncMode syncMode) throws IOException {
        String mode;
        switch (rwMode) {
            case READ_ONLY:
                mode = "r";
                break;

            case READ_WRITE:
                fileRef.getFile().getAbsoluteFile().getParentFile().mkdirs();
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
        raf = new RandomAccessFile(fileRef.getFile(), mode);
        channel = raf.getChannel();
    }

    public void close() throws IOException {
        channel.close();
        raf.close();
    }

    public FileReference getFileReference() {
        return fileRef;
    }

    public RandomAccessFile getRandomAccessFile() {
        return raf;
    }

    public FileChannel getFileChannel() {
        return channel;
    }

    public void sync(boolean metadata) throws IOException {
        channel.force(metadata);
    }
}