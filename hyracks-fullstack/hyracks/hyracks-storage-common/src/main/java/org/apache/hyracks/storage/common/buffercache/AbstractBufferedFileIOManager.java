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
package org.apache.hyracks.storage.common.buffercache;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.common.compression.file.CompressedFileReference;
import org.apache.hyracks.storage.common.compression.file.ICompressedPageWriter;
import org.apache.hyracks.util.annotations.NotThreadSafe;

/**
 * Handles all IO operations for a specified file.
 */
@NotThreadSafe
public abstract class AbstractBufferedFileIOManager {
    private static final String ERROR_MESSAGE = "%s unexpected number of bytes: [expected: %d, actual: %d, file: %s]";
    private static final String READ = "Read";
    private static final String WRITE = "Written";

    protected final BufferCache bufferCache;
    protected final IPageReplacementStrategy pageReplacementStrategy;
    private final BlockingQueue<BufferCacheHeaderHelper> headerPageCache;
    private final IOManager ioManager;

    private IFileHandle fileHandle;
    private volatile boolean hasOpen;

    protected AbstractBufferedFileIOManager(BufferCache bufferCache, IIOManager ioManager,
            BlockingQueue<BufferCacheHeaderHelper> headerPageCache, IPageReplacementStrategy pageReplacementStrategy) {
        this.bufferCache = bufferCache;
        this.ioManager = (IOManager) ioManager;
        this.headerPageCache = headerPageCache;
        this.pageReplacementStrategy = pageReplacementStrategy;
        hasOpen = false;
    }

    /* ********************************
     * Read/Write page methods
     * ********************************
     */

    /**
     * Read the CachedPage from disk
     *
     * @param cPage
     *            CachedPage in {@link BufferCache}
     * @throws HyracksDataException
     */
    public abstract void read(CachedPage cPage) throws HyracksDataException;

    /**
     * Write the CachedPage into disk
     *
     * @param cPage
     *            CachedPage in {@link BufferCache}
     * @throws HyracksDataException
     */
    public void write(CachedPage cPage) throws HyracksDataException {
        final int totalPages = cPage.getFrameSizeMultiplier();
        final int extraBlockPageId = cPage.getExtraBlockPageId();
        final BufferCacheHeaderHelper header = checkoutHeaderHelper();
        write(cPage, header, totalPages, extraBlockPageId);
    }

    /**
     * Write the CachedPage into disk called by {@link AbstractBufferedFileIOManager#write(CachedPage)}
     * Note: It is the responsibility of the caller to return {@link BufferCacheHeaderHelper}
     *
     * @param cPage
     *            CachedPage that will be written
     * @param header
     *            HeaderHelper to add into the written page
     * @param totalPages
     *            Number of pages to be written
     * @param extraBlockPageId
     *            Extra page ID in case it has more than one page
     * @throws HyracksDataException
     */
    protected abstract void write(CachedPage cPage, BufferCacheHeaderHelper header, int totalPages,
            int extraBlockPageId) throws HyracksDataException;

    /* ********************************
     * File operations' methods
     * ********************************
     */

    /**
     * Open the file
     *
     * @throws HyracksDataException
     */
    public void open(FileReference fileRef) throws HyracksDataException {
        fileHandle = ioManager.open(fileRef, IIOManager.FileReadWriteMode.READ_WRITE,
                IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        hasOpen = true;
    }

    /**
     * Close the file
     *
     * @throws HyracksDataException
     */
    public void close() throws HyracksDataException {
        if (hasOpen) {
            ioManager.close(fileHandle);
        }
    }

    public void purge() throws HyracksDataException {
        ioManager.close(fileHandle);
    }

    /**
     * Force the file into disk
     *
     * @param metadata
     *            see {@link java.nio.channels.FileChannel#force(boolean)}
     * @throws HyracksDataException
     */
    public void force(boolean metadata) throws HyracksDataException {
        ioManager.sync(fileHandle, metadata);
    }

    /**
     * Get the number of pages in the file
     *
     * @throws HyracksDataException
     */
    public abstract int getNumberOfPages() throws HyracksDataException;

    public void markAsDeleted() throws HyracksDataException {
        fileHandle = null;
    }

    /**
     * Check whether the file has been deleted
     *
     * @return
     *         true if has been deleted, false o.w
     */
    public boolean hasBeenDeleted() {
        return fileHandle == null;
    }

    /**
     * Check whether the file has ever been opened
     *
     * @return
     *         true if has ever been opened, false otherwise
     */
    public final boolean hasBeenOpened() {
        return hasOpen;
    }

    public final FileReference getFileReference() {
        return fileHandle.getFileReference();
    }

    public static void createFile(BufferCache bufferCache, FileReference fileRef) throws HyracksDataException {
        IoUtil.create(fileRef);
        if (fileRef.isCompressed()) {
            final CompressedFileReference cFileRef = (CompressedFileReference) fileRef;
            try {
                bufferCache.createFile(cFileRef.getLAFFileReference());
            } catch (HyracksDataException e) {
                //In case of creating the LAF file failed, delete index file reference
                IoUtil.delete(fileRef);
                throw e;
            }
        }
    }

    public static void deleteFile(FileReference fileRef) throws HyracksDataException {
        HyracksDataException savedEx = null;

        /*
         * LAF file has to be deleted before the index file.
         * If the index file deleted first and a non-graceful shutdown happened before the deletion of
         * the LAF file, the LAF file will not be deleted during the next recovery.
         */
        try {
            if (fileRef.isCompressed()) {
                final CompressedFileReference cFileRef = (CompressedFileReference) fileRef;
                final FileReference lafFileRef = cFileRef.getLAFFileReference();
                if (lafFileRef.getFile().exists()) {
                    IoUtil.delete(lafFileRef);
                }
            }
        } catch (HyracksDataException e) {
            savedEx = e;
        }

        try {
            IoUtil.delete(fileRef);
        } catch (HyracksDataException e) {
            if (savedEx != null) {
                savedEx.addSuppressed(e);
            } else {
                savedEx = e;
            }
        }

        if (savedEx != null) {
            throw savedEx;
        }
    }

    /* ********************************
     * Compressed file methods
     * ********************************
     */

    public abstract ICompressedPageWriter getCompressedPageWriter();

    /* ********************************
     * Common helper methods
     * ********************************
     */

    /**
     * Get the offset for the first page
     *
     * @param cPage
     *            CachedPage for which the offset is needed
     * @return
     *         page offset in the file
     */
    protected abstract long getFirstPageOffset(CachedPage cPage);

    /**
     * Get the offset for the extra page
     *
     * @param cPage
     *            CachedPage for which the offset is needed
     * @return
     *         page offset in the file
     */
    protected abstract long getExtraPageOffset(CachedPage cPage);

    protected final BufferCacheHeaderHelper checkoutHeaderHelper() {
        BufferCacheHeaderHelper helper = headerPageCache.poll();
        if (helper == null) {
            helper = new BufferCacheHeaderHelper(bufferCache.getPageSize());
        }
        return helper;
    }

    protected final void returnHeaderHelper(BufferCacheHeaderHelper buffer) {
        headerPageCache.offer(buffer); //NOSONAR
    }

    protected final long readToBuffer(ByteBuffer buf, long offset) throws HyracksDataException {
        return ioManager.syncRead(fileHandle, offset, buf);
    }

    protected final long writeToFile(ByteBuffer buf, long offset) throws HyracksDataException {
        return ioManager.doSyncWrite(fileHandle, offset, buf);
    }

    protected final long writeToFile(ByteBuffer[] buf, long offset) throws HyracksDataException {
        return ioManager.doSyncWrite(fileHandle, offset, buf);
    }

    protected final long getFileSize() {
        return ioManager.getSize(fileHandle);
    }

    protected final void verifyBytesWritten(long expected, long actual) {
        if (expected != actual) {
            throwException(WRITE, expected, actual);
        }
    }

    protected final boolean verifyBytesRead(long expected, long actual) {
        if (expected != actual) {
            if (actual == -1) {
                // disk order scan code seems to rely on this behavior, so silently return
                return false;
            } else {
                throwException(READ, expected, actual);
            }
        }
        return true;
    }

    protected void throwException(String op, long expected, long actual) {
        final String path = fileHandle.getFileReference().getAbsolutePath();
        throw new IllegalStateException(String.format(ERROR_MESSAGE, op, expected, actual, path));
    }
}
