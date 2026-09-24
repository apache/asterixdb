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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.control.nc.io.FileHandle;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheWriteContext;
import org.apache.hyracks.storage.common.compression.file.CompressedFileReference;
import org.apache.hyracks.storage.common.compression.file.ICompressedPageWriter;
import org.apache.hyracks.util.IThreadStats;
import org.apache.hyracks.util.annotations.NotThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Handles all IO operations for a specified file.
 * <p>
 * The OS descriptor behind the file is separate from the file's lifecycle in the {@link BufferCache}: an open file
 * whose descriptor has been released by {@link #tryReleaseDescriptor()} stays open as far as the cache is concerned,
 * keeping its file id, pages and reference count, and has its descriptor reopened by the next read, write or force.
 * Every such operation holds the descriptor lock shared, so a descriptor is never released under an I/O in flight.
 */
@NotThreadSafe
public abstract class AbstractBufferedFileIOManager {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final String ERROR_MESSAGE = "%s unexpected number of bytes: [expected: %d, actual: %d, file: %s]";
    private static final String READ = "Read";
    private static final String WRITE = "Written";

    protected final BufferCache bufferCache;
    protected final IPageReplacementStrategy pageReplacementStrategy;
    protected final IOManager ioManager;
    private final BlockingQueue<BufferCacheHeaderHelper> headerPageCache;

    private IFileHandle fileHandle;
    private volatile boolean hasOpen;

    // shared by every I/O on the descriptor, exclusive to release it; not reentrant, and no I/O here nests another
    // on the same file
    private final StampedLock descriptorLock = new StampedLock();
    // serializes reopening among the readers sharing descriptorLock; private, because the buffer cache synchronizes
    // on the handle itself while closing it, which waits on descriptorLock
    private final Object descriptorMonitor = new Object();
    // whether the OS descriptor is currently held; changed under descriptorLock exclusively, or under descriptorLock
    // shared plus descriptorMonitor when reopening
    private volatile boolean descriptorOpen;
    // set once the file is closed, purged or deleted, after which its descriptor is never reopened
    private volatile boolean retired;
    // writes issued, and the count covered by the last successful force: a descriptor is only released when equal
    private final AtomicLong writeCount = new AtomicLong();
    private volatile long forcedWriteCount;
    private volatile long lastIoNanos;
    // System.nanoTime() as of the last release of the descriptor, for logging how long it stayed released
    private volatile long releasedNanos;

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
     * @param cPage   CachedPage in {@link BufferCache}
     * @param context read context
     */
    public abstract void read(CachedPage cPage, IBufferCacheReadContext context, IThreadStats threadStats)
            throws HyracksDataException;

    /**
     * Write the CachedPage into disk
     *
     * @param cPage   CachedPage in {@link BufferCache}
     * @param context write context
     */
    public void write(CachedPage cPage, IBufferCacheWriteContext context) throws HyracksDataException {
        final int totalPages = cPage.getFrameSizeMultiplier();
        final int extraBlockPageId = cPage.getExtraBlockPageId();
        final BufferCacheHeaderHelper header = checkoutHeaderHelper();
        write(cPage, header, totalPages, extraBlockPageId, context);
    }

    /**
     * Write the CachedPage into disk called by
     * {@link AbstractBufferedFileIOManager#write(CachedPage, IBufferCacheWriteContext)}
     * Note: It is the responsibility of the caller to return {@link BufferCacheHeaderHelper}
     *
     * @param cPage            CachedPage that will be written
     * @param header           HeaderHelper to add into the written page
     * @param totalPages       Number of pages to be written
     * @param extraBlockPageId Extra page ID in case it has more than one page
     * @param context          write context
     */
    protected abstract void write(CachedPage cPage, BufferCacheHeaderHelper header, int totalPages,
            int extraBlockPageId, IBufferCacheWriteContext context) throws HyracksDataException;

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
        final long stamp = descriptorLock.writeLock();
        try {
            fileHandle = ioManager.open(fileRef, IIOManager.FileReadWriteMode.READ_WRITE,
                    IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
            lastIoNanos = System.nanoTime();
            retired = false;
            descriptorOpen = true;
            hasOpen = true;
        } finally {
            descriptorLock.unlockWrite(stamp);
        }
        bufferCache.descriptorOpened();
    }

    /**
     * Close the file
     *
     * @throws HyracksDataException
     */
    public void close() throws HyracksDataException {
        if (hasOpen) {
            retireDescriptor("closed");
        }
    }

    public void purge() throws HyracksDataException {
        retireDescriptor("purged");
    }

    /**
     * @return the file's OS handle. I/O done on it directly bypasses the descriptor lock, so a caller doing so must
     *         only run where the buffer cache does not release descriptors
     */
    public IFileHandle getFileHandle() {
        return fileHandle;
    }

    /**
     * Force the file into disk
     *
     * @param metadata see {@link java.nio.channels.FileChannel#force(boolean)}
     * @throws HyracksDataException
     */
    public void force(boolean metadata) throws HyracksDataException {
        // sampled before the sync: a write that lands during it is not known to be covered by it
        final long writesCovered = writeCount.get();
        if (!descriptorOpen && writesCovered == forcedWriteCount) {
            // released only once everything written was forced, and nothing has been written since
            return;
        }
        final long stamp = beginIo();
        try {
            ioManager.sync(fileHandle, metadata);
        } finally {
            endIo(stamp);
        }
        synchronized (descriptorMonitor) {
            if (writesCovered > forcedWriteCount) {
                forcedWriteCount = writesCovered;
            }
        }
    }

    /* ********************************
     * OS descriptor management
     * ********************************
     */

    /**
     * Release this file's OS descriptor if nothing is using it and everything written through it has been forced.
     * The file stays open in the {@link BufferCache}; the next I/O reopens the descriptor. Never blocks: a descriptor
     * with an I/O in flight is left alone.
     *
     * @return true if a descriptor was released
     */
    public boolean tryReleaseDescriptor() {
        if (!isDescriptorReleasable()) {
            return false;
        }
        final long stamp = descriptorLock.tryWriteLock();
        if (stamp == 0L) {
            return false;
        }
        try {
            if (!isDescriptorReleasable()) {
                return false;
            }
            ioManager.close(fileHandle);
            descriptorOpen = false;
            releasedNanos = System.nanoTime();
        } catch (HyracksDataException e) {
            // leave the descriptor as it was; it is not in use, and the next pass will try again
            LOGGER.debug("failed to release the file descriptor of {}", fileHandle.getFileReference(), e);
            return false;
        } finally {
            descriptorLock.unlockWrite(stamp);
        }
        final long idleNanos = releasedNanos - lastIoNanos;
        final int open = bufferCache.descriptorReleased(idleNanos);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("released the file descriptor of {} after {}ms idle; {} now open", getFileReference(),
                    TimeUnit.NANOSECONDS.toMillis(idleNanos), open);
        }
        return true;
    }

    /**
     * @return whether the OS descriptor is currently held
     */
    public final boolean isDescriptorOpen() {
        return descriptorOpen;
    }

    /**
     * @return {@link System#nanoTime()} as of the last I/O through this file, or of its opening
     */
    public final long getLastIoNanos() {
        return lastIoNanos;
    }

    private boolean isDescriptorReleasable() {
        return descriptorOpen && !retired && fileHandle != null && writeCount.get() == forcedWriteCount;
    }

    private void retireDescriptor(String reason) throws HyracksDataException {
        // exclusive, and blocking: the file is being closed for good, so wait out any I/O still in flight
        final long stamp = descriptorLock.writeLock();
        final IFileHandle retiredHandle = fileHandle;
        boolean wasOpen;
        try {
            retired = true;
            wasOpen = descriptorOpen;
            descriptorOpen = false;
            if (fileHandle != null) {
                ioManager.close(fileHandle);
            }
        } finally {
            descriptorLock.unlockWrite(stamp);
        }
        final int open = wasOpen ? bufferCache.descriptorClosed() : bufferCache.getOpenDescriptorCount();
        if (retiredHandle != null && LOGGER.isDebugEnabled()) {
            LOGGER.debug("retired the file descriptor of {} ({}; descriptor {}); {} now open",
                    retiredHandle.getFileReference(), reason, wasOpen ? "held" : "already released", open);
        }
    }

    private long beginIo() throws HyracksDataException {
        final long stamp = descriptorLock.readLock();
        try {
            if (!descriptorOpen) {
                reopenDescriptor();
            }
            lastIoNanos = System.nanoTime();
            return stamp;
        } catch (Throwable th) {
            descriptorLock.unlockRead(stamp);
            throw th;
        }
    }

    private void endIo(long stamp) {
        descriptorLock.unlockRead(stamp);
    }

    private void reopenDescriptor() throws HyracksDataException {
        boolean reopened = false;
        long reopenedNanos = 0L;
        // several readers may get here at once under the shared lock; only one of them reopens
        synchronized (descriptorMonitor) {
            if (!descriptorOpen && !retired && fileHandle != null) {
                ((FileHandle) fileHandle).ensureOpen();
                descriptorOpen = true;
                reopened = true;
                reopenedNanos = System.nanoTime();
            }
        }
        if (reopened) {
            final long releasedForNanos = reopenedNanos - releasedNanos;
            final int open = bufferCache.descriptorReopened(releasedForNanos);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("reopened the file descriptor of {} after {}ms released; {} now open", getFileReference(),
                        TimeUnit.NANOSECONDS.toMillis(releasedForNanos), open);
            }
        }
    }

    /**
     * Return start offset of a page
     *
     * @param pageId page ID
     * @return offset
     */
    public abstract long getStartPageOffset(int pageId) throws HyracksDataException;

    /**
     * Get the number of pages in the file
     *
     * @throws HyracksDataException
     */
    public abstract int getNumberOfPages() throws HyracksDataException;

    public void markAsDeleted() throws HyracksDataException {
        retired = true;
        fileHandle = null;
    }

    /**
     * Check whether the file has been deleted
     *
     * @return true if has been deleted, false o.w
     */
    public boolean hasBeenDeleted() {
        return fileHandle == null;
    }

    /**
     * Check whether the file has ever been opened
     *
     * @return true if has ever been opened, false otherwise
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

    public static void deleteFile(FileReference fileRef, IIOManager ioManager) throws HyracksDataException {
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
                    ioManager.delete(lafFileRef);
                }
            }
        } catch (HyracksDataException e) {
            savedEx = e;
        }

        try {
            ioManager.delete(fileRef);
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

    /**
     * Compute the total size of pages
     *
     * @param startPageId   page ID to start from
     * @param numberOfPages the number of pages
     * @return total size of pages in bytes
     */
    public abstract long getPagesTotalSize(int startPageId, int numberOfPages) throws HyracksDataException;

    /* ********************************
     * Common helper methods
     * ********************************
     */

    /**
     * Get the offset for the first page
     *
     * @param cPage CachedPage for which the offset is needed
     * @return page offset in the file
     */
    protected abstract long getFirstPageOffset(CachedPage cPage);

    /**
     * Get the offset for the extra page
     *
     * @param cPage CachedPage for which the offset is needed
     * @return page offset in the file
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
        final long stamp = beginIo();
        try {
            return ioManager.syncRead(fileHandle, offset, buf);
        } finally {
            endIo(stamp);
        }
    }

    /**
     * Read a page's header from the file, under the descriptor lock.
     */
    protected final long readHeaderFromFile(BufferCacheHeaderHelper header, long offset, int size)
            throws HyracksDataException {
        final long stamp = beginIo();
        try {
            return header.readFromFile(ioManager, fileHandle, offset, size);
        } finally {
            endIo(stamp);
        }
    }

    /**
     * Write through the given context, under the descriptor lock.
     */
    protected final long writeToFile(IBufferCacheWriteContext context, long offset, ByteBuffer buf)
            throws HyracksDataException {
        // counted before the write, so a force that overlaps it cannot claim to cover it
        writeCount.incrementAndGet();
        final long stamp = beginIo();
        try {
            return context.write(ioManager, fileHandle, offset, buf);
        } finally {
            endIo(stamp);
        }
    }

    protected final long writeToFile(IBufferCacheWriteContext context, long offset, ByteBuffer[] buf)
            throws HyracksDataException {
        writeCount.incrementAndGet();
        final long stamp = beginIo();
        try {
            return context.write(ioManager, fileHandle, offset, buf);
        } finally {
            endIo(stamp);
        }
    }

    protected final long writeExtraToFile(ByteBuffer buf, long offset) throws HyracksDataException {
        writeCount.incrementAndGet();
        final long stamp = beginIo();
        try {
            return ioManager.doSyncWrite(fileHandle, offset, buf);
        } finally {
            endIo(stamp);
        }
    }

    protected final long writeExtraToFile(ByteBuffer[] buf, long offset) throws HyracksDataException {
        writeCount.incrementAndGet();
        final long stamp = beginIo();
        try {
            return ioManager.doSyncWrite(fileHandle, offset, buf);
        } finally {
            endIo(stamp);
        }
    }

    protected final long getFileSize() throws HyracksDataException {
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

    @Override
    public String toString() {
        return fileHandle != null ? fileHandle.getFileReference().getAbsolutePath() : "";
    }
}
