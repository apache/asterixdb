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

    /**
     * @return IO devices in an NC
     */
    List<IODeviceHandle> getIODevices();

    /**
     * Open file
     *
     * @param fileRef  file reference
     * @param rwMode   r/w mode
     * @param syncMode sync mode
     * @return file handle for the opened file
     */
    IFileHandle open(FileReference fileRef, FileReadWriteMode rwMode, FileSyncMode syncMode)
            throws HyracksDataException;

    /**
     * Do sync write (utilizes {@link IAsyncRequest})
     *
     * @param fHandle handle of the opened file
     * @param offset  start offset
     * @param data    buffer to write
     * @return number of written bytes
     */
    int syncWrite(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException;

    /**
     * Do sync write (utilizes {@link IAsyncRequest})
     *
     * @param fHandle   file handle of the opened file
     * @param offset    start offset
     * @param dataArray buffers to write
     * @return number of written bytes
     */
    long syncWrite(IFileHandle fHandle, long offset, ByteBuffer[] dataArray) throws HyracksDataException;

    /**
     * Do sync read (utilizes {@link IAsyncRequest})
     *
     * @param fHandle handle of the opened file
     * @param offset  start offset
     * @param data    destination buffer for the requested content
     * @return number of read bytes
     */
    int syncRead(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException;

    /**
     * Do async write
     *
     * @param fHandle handle of the opened file
     * @param offset  start offset
     * @param data    buffer to write
     * @return IAsyncRequest which allows to wait for the write request
     */
    IAsyncRequest asyncWrite(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException;

    /**
     * Do async write
     *
     * @param fHandle   handle of the opened file
     * @param offset    start offset
     * @param dataArray buffers to write
     * @return IAsyncRequest which allows to wait for the write request
     */
    IAsyncRequest asyncWrite(IFileHandle fHandle, long offset, ByteBuffer[] dataArray) throws HyracksDataException;

    /**
     * Do async read
     *
     * @param fHandle handle of the opened file
     * @param offset  start offset
     * @param data    destination buffer for the requested content
     * @return IAsyncRequest which allows to wait for the read request
     */
    IAsyncRequest asyncRead(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException;

    /**
     * Close file
     *
     * @param fHandle handle of the opened file
     */
    void close(IFileHandle fHandle) throws HyracksDataException;

    /**
     * Forces the content of the opened file
     *
     * @param fileHandle handle of the opened file
     * @param metadata   force the file metadata as well
     */
    void sync(IFileHandle fileHandle, boolean metadata) throws HyracksDataException;

    /**
     * Truncates the opened file to the requested size
     *
     * @param fileHandle handle of the opened file
     * @param size       required size
     */
    void truncate(IFileHandle fileHandle, long size) throws HyracksDataException;

    /**
     * Gets the size of an opened file
     *
     * @param fileHandle handle of the opened file
     * @return file size
     */
    long getSize(IFileHandle fileHandle) throws HyracksDataException;

    /**
     * Gets the size of a file
     *
     * @param fileReference file reference
     * @return file size
     */
    long getSize(FileReference fileReference) throws HyracksDataException;

    /**
     * Returns a new write channel
     *
     * @param fileHandle handle of the opened file
     * @return a new write channel
     */
    WritableByteChannel newWritableChannel(IFileHandle fileHandle);

    void deleteWorkspaceFiles() throws HyracksDataException;

    /**
     * Returns a file reference of a file
     *
     * @param ioDeviceId device Id
     * @param path       relative path
     * @return A file reference based on the mounting point of {@code ioDeviceId} and the passed {@code relativePath}
     */
    FileReference getFileReference(int ioDeviceId, String path);

    /**
     * determine which IO device holds the path and returns a FileReference based on that
     *
     * @param path relative path
     * @return A file reference based on the mounting point of {@code ioDeviceId} and the passed {@code Path}
     */
    FileReference resolve(String path) throws HyracksDataException;

    /**
     * Gets a file reference from an absolute path
     *
     * @param path absolute path
     * @return A file reference based on the mounting point of {@code ioDeviceId} and the passed {@code relativePath}
     * @deprecated use getFileRef(int ioDeviceId, String path) instead
     */
    @Deprecated
    FileReference resolveAbsolutePath(String path) throws HyracksDataException;

    /**
     * Create a workspace file with the given prefix
     *
     * @param prefix of workspace file
     * @return A FileReference for the created workspace file
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
     * @param fileRef file/directory to delete
     */
    void delete(FileReference fileRef) throws HyracksDataException;

    /**
     * @return bulk-delete operation (for either file or directories)
     */
    IIOBulkOperation createDeleteBulkOperation();

    /**
     * List of files in the directory and its subdirectories
     *
     * @param dir directory to list
     * @return a set of all files in the directory and its subdirectories
     */
    Set<FileReference> list(FileReference dir) throws HyracksDataException;

    /**
     * List of files in the directory and its subdirectories that satisfy the provided filter
     *
     * @param dir    directory to list
     * @param filter to test if a file reference satisfies a certain predicate
     * @return a set of all files in the directory and its subdirectories that satisfies the provided filter
     */
    Set<FileReference> list(FileReference dir, FilenameFilter filter) throws HyracksDataException;

    /**
     * Overwrites (or write) a file
     *
     * @param fileRef file reference of the file to overwrite
     * @param bytes   content
     */
    void overwrite(FileReference fileRef, byte[] bytes) throws HyracksDataException;

    /**
     * Reads the entire content of a file
     *
     * @param fileRef file reference
     * @return a byte array of the content
     */
    byte[] readAllBytes(FileReference fileRef) throws HyracksDataException;

    /**
     * Copy the content of one directory to another
     *
     * @param srcDir  source directory
     * @param destDir destination directory
     */
    void copyDirectory(FileReference srcDir, FileReference destDir) throws HyracksDataException;

    /**
     * Checks whether a file exists
     *
     * @param fileRef file reference
     * @return true if the file exists, false otherwise
     */
    boolean exists(FileReference fileRef) throws HyracksDataException;

    /**
     * Creates a file
     *
     * @param fileRef file reference
     */
    void create(FileReference fileRef) throws HyracksDataException;

    /**
     * Make a directory and all of its parent directories
     *
     * @param dir directory to create
     * @return true of it was created
     */
    boolean makeDirectories(FileReference dir);

    /**
     * Deletes the content of a directory (files and subdirectories)
     *
     * @param dir directory reference
     */
    void cleanDirectory(FileReference dir) throws HyracksDataException;

    /**
     * Performs a bulk operation
     *
     * @param bulkOperation the operation to perform
     */
    void performBulkOperation(IIOBulkOperation bulkOperation) throws HyracksDataException;
}
