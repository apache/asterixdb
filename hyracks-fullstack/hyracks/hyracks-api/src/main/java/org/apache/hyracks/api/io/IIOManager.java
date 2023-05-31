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
import java.io.FilenameFilter;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IIOManager extends Closeable {

    enum FileReadWriteMode {
        READ_ONLY,
        READ_WRITE
    }

    enum FileSyncMode {
        METADATA_SYNC_DATA_SYNC,
        METADATA_ASYNC_DATA_SYNC,
        METADATA_ASYNC_DATA_ASYNC
    }

    List<IODeviceHandle> getIODevices();

    IFileHandle open(FileReference fileRef, FileReadWriteMode rwMode, FileSyncMode syncMode)
            throws HyracksDataException;

    int syncWrite(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException;

    long syncWrite(IFileHandle fHandle, long offset, ByteBuffer[] dataArray) throws HyracksDataException;

    int syncRead(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException;

    IAsyncRequest asyncWrite(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException;

    IAsyncRequest asyncWrite(IFileHandle fHandle, long offset, ByteBuffer[] dataArray) throws HyracksDataException;

    IAsyncRequest asyncRead(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException;

    void close(IFileHandle fHandle) throws HyracksDataException;

    void sync(IFileHandle fileHandle, boolean metadata) throws HyracksDataException;

    void truncate(IFileHandle fileHandle, long size) throws HyracksDataException;

    long getSize(IFileHandle fileHandle) throws HyracksDataException;

    long getSize(FileReference fileReference) throws HyracksDataException;

    WritableByteChannel newWritableChannel(IFileHandle fileHandle);

    void deleteWorkspaceFiles() throws HyracksDataException;

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
     * @param path
     * @return A file reference based on the mounting point of {@code ioDeviceId} and the passed {@code relativePath}
     * @throws HyracksDataException
     * @deprecated use getFileRef(int ioDeviceId, String path) instead
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

    /**
     * Delete any additional artifacts associated with the file reference
     *
     * @param fileRef
     */
    void delete(FileReference fileRef) throws HyracksDataException;

    Set<FileReference> list(FileReference dir) throws HyracksDataException;

    /**
     * Lists the files matching {@code filter} recursively starting from {@code dir}
     * @param dir
     * @param filter
     * @return the matching files
     * @throws HyracksDataException
     */
    Set<FileReference> list(FileReference dir, FilenameFilter filter) throws HyracksDataException;

    void overwrite(FileReference fileRef, byte[] bytes) throws ClosedByInterruptException, HyracksDataException;

    byte[] readAllBytes(FileReference fileRef) throws HyracksDataException;

    void copyDirectory(FileReference srcMetadataScopePath, FileReference targetMetadataScopePath)
            throws HyracksDataException;

    void deleteDirectory(FileReference root) throws HyracksDataException;

    boolean exists(FileReference fileRef) throws HyracksDataException;

    void create(FileReference fileRef) throws HyracksDataException;

    boolean makeDirectories(FileReference resourceDir);

    void cleanDirectory(FileReference resourceDir) throws HyracksDataException;

    void syncFiles(Set<Integer> activePartitions) throws HyracksDataException;
}
