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
package edu.uci.ics.hyracks.api.io;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executor;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IIOManager {
    public enum FileReadWriteMode {
        READ_ONLY,
        READ_WRITE
    }

    public enum FileSyncMode {
        METADATA_SYNC_DATA_SYNC,
        METADATA_ASYNC_DATA_SYNC,
        METADATA_ASYNC_DATA_ASYNC
    }

    public List<IODeviceHandle> getIODevices();

    public IFileHandle open(FileReference fileRef, FileReadWriteMode rwMode, FileSyncMode syncMode)
            throws HyracksDataException;

    public int syncWrite(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException;

    public int syncRead(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException;

    public IIOFuture asyncWrite(IFileHandle fHandle, long offset, ByteBuffer data);

    public IIOFuture asyncRead(IFileHandle fHandle, long offset, ByteBuffer data);

    public void close(IFileHandle fHandle) throws HyracksDataException;

    public void sync(IFileHandle fileHandle, boolean metadata) throws HyracksDataException;

    public void setExecutor(Executor executor);
}