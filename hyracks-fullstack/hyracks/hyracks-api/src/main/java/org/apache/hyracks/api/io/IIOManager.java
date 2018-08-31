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
package org.apache.hyracks.api.io;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IIOManager extends Closeable {
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

    public long syncWrite(IFileHandle fHandle, long offset, ByteBuffer[] dataArray) throws HyracksDataException;

    public int syncRead(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException;

    IAsyncRequest asyncWrite(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException;

    IAsyncRequest asyncWrite(IFileHandle fHandle, long offset, ByteBuffer[] dataArray) throws HyracksDataException;

    IAsyncRequest asyncRead(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException;

    public void close(IFileHandle fHandle) throws HyracksDataException;

    public void sync(IFileHandle fileHandle, boolean metadata) throws HyracksDataException;

    public long getSize(IFileHandle fileHandle);

    public void deleteWorkspaceFiles() throws HyracksDataException;

    /**
     * @param ioDeviceId
     * @param path
     * @return A file reference based on the mounting point of {@code ioDeviceId} and the passed {@code relativePath}
     */
    FileReference getFileReference(int ioDeviceId, String path);

    /**
     * determine which IO device holds the path and returns a FileReference based on that
     *
     * @param path
     * @return A file reference based on the mounting point of {@code ioDeviceId} and the passed {@code Path}
     * @throws HyracksDataException
     */
    FileReference resolve(String path) throws HyracksDataException;

    /**
     * Gets a file reference from an absolute path
     *
     * @deprecated
     *             use getFileRef(int ioDeviceId, String path) instead
     * @param path
     * @return A file reference based on the mounting point of {@code ioDeviceId} and the passed {@code relativePath}
     * @throws HyracksDataException
     */
    @Deprecated
    FileReference resolveAbsolutePath(String path) throws HyracksDataException;

    /**
     * Create a workspace file with the given prefix
     *
     * @param prefix
     * @return A FileReference for the created workspace file
     * @throws HyracksDataException
     */
    FileReference createWorkspaceFile(String prefix) throws HyracksDataException;

    /**
     * Gets the total disk usage in bytes of this {@link IIOManager} io devices handles.
     *
     * @return the total disk usage in bytes
     */
    long getTotalDiskUsage();
}
