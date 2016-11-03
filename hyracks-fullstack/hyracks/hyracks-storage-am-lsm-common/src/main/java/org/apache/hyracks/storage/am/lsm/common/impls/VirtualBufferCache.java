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
package org.apache.hyracks.storage.am.lsm.common.impls;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.replication.IIOReplicationManager;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageQueue;
import org.apache.hyracks.storage.common.buffercache.IQueueInfo;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.storage.common.file.IFileMapManager;
import org.apache.hyracks.storage.common.file.TransientFileMapManager;

public class VirtualBufferCache implements IVirtualBufferCache {
    private static final Logger LOGGER = Logger.getLogger(ExternalIndexHarness.class.getName());

    private static final int OVERFLOW_PADDING = 8;

    private final ICacheMemoryAllocator allocator;
    private final IFileMapManager fileMapManager;
    private final int pageSize;
    private final int numPages;

    private final CacheBucket[] buckets;
    private final ArrayList<VirtualPage> pages;

    private volatile int nextFree;
    private final AtomicInteger largePages;

    private boolean open;

    public VirtualBufferCache(ICacheMemoryAllocator allocator, int pageSize, int numPages) {
        this.allocator = allocator;
        this.fileMapManager = new TransientFileMapManager();
        this.pageSize = pageSize;
        this.numPages = 2 * (numPages / 2) + 1;

        buckets = new CacheBucket[this.numPages];
        pages = new ArrayList<>();
        nextFree = 0;
        largePages = new AtomicInteger(0);
        open = false;
    }

    @Override
    public void createFile(FileReference fileRef) throws HyracksDataException {
        synchronized (fileMapManager) {
            if (fileMapManager.isMapped(fileRef)) {
                throw new HyracksDataException("File " + fileRef + " is already mapped");
            }
            fileMapManager.registerFile(fileRef);
        }
    }

    @Override
    public void openFile(int fileId) throws HyracksDataException {
    }

    @Override
    public void closeFile(int fileId) throws HyracksDataException {
    }

    @Override
    public void deleteFile(int fileId, boolean flushDirtyPages) throws HyracksDataException {
        synchronized (fileMapManager) {
            if (!fileMapManager.isMapped(fileId)) {
                throw new HyracksDataException("File with id " + fileId + " is not mapped");
            }
            fileMapManager.unregisterFile(fileId);
        }

        for (int i = 0; i < buckets.length; i++) {
            final CacheBucket bucket = buckets[i];
            bucket.bucketLock.lock();
            try {
                VirtualPage prev = null;
                VirtualPage curr = bucket.cachedPage;
                while (curr != null) {
                    if (BufferedFileHandle.getFileId(curr.dpid) == fileId) {
                        if (prev == null) {
                            bucket.cachedPage = curr.next;
                            curr.reset();
                            curr = bucket.cachedPage;
                        } else {
                            prev.next = curr.next;
                            curr.reset();
                            curr = prev.next;
                        }
                    } else {
                        prev = curr;
                        curr = curr.next;
                    }
                }
            } finally {
                bucket.bucketLock.unlock();
            }
        }
        defragPageList();
    }

    private void defragPageList() {
        synchronized (pages) {
            int start = 0;
            int end = nextFree - 1;
            while (start < end) {
                VirtualPage lastUsed = pages.get(end);
                while (end > 0 && lastUsed.dpid == -1) {
                    --end;
                    lastUsed = pages.get(end);
                }

                if (end == 0) {
                    nextFree = lastUsed.dpid == -1 ? 0 : 1;
                    break;
                }

                VirtualPage firstUnused = pages.get(start);
                while (start < end && firstUnused.dpid != -1) {
                    ++start;
                    firstUnused = pages.get(start);
                }

                if (start >= end) {
                    break;
                }

                Collections.swap(pages, start, end);
                nextFree = end;
                --end;
                ++start;
            }
        }
    }

    @Override
    public ICachedPage tryPin(long dpid) throws HyracksDataException {
        return pin(dpid, false);
    }

    @Override
    public ICachedPage pin(long dpid, boolean newPage) throws HyracksDataException {
        VirtualPage page = null;
        int hash = hash(dpid);
        CacheBucket bucket = buckets[hash];

        bucket.bucketLock.lock();
        try {
            page = bucket.cachedPage;
            while (page != null) {
                if (page.dpid == dpid) {
                    return page;
                }
                page = page.next;
            }

            if (!newPage) {
                throw new HyracksDataException("Page " + BufferedFileHandle.getPageId(dpid) + " does not exist in file "
                        + fileMapManager.lookupFileName(BufferedFileHandle.getFileId(dpid)));
            }

            page = getOrAllocPage(dpid);
            page.next = bucket.cachedPage;
            bucket.cachedPage = page;
        } finally {
            bucket.bucketLock.unlock();
        }

        return page;
    }

    private int hash(long dpid) {
        int hashValue = (int) dpid ^ (Integer.reverse((int) (dpid >>> 32)) >>> 1);
        return hashValue % buckets.length;
    }

    private VirtualPage getOrAllocPage(long dpid) {
        VirtualPage page;
        synchronized (pages) {
            if (nextFree >= pages.size()) {
                page = new VirtualPage(allocator.allocate(pageSize, 1)[0]);
                page.multiplier = 1;
                pages.add(page);
            } else {
                page = pages.get(nextFree);
            }
            ++nextFree;
            page.dpid = dpid;
        }
        return page;
    }

    @Override
    public void resizePage(ICachedPage cPage, int multiplier, IExtraPageBlockHelper extraPageBlockHelper) {
        ByteBuffer oldBuffer = cPage.getBuffer();
        int origMultiplier = cPage.getFrameSizeMultiplier();
        if (origMultiplier == multiplier) {
            // no-op
            return;
        }
        if (origMultiplier == 1) {
            synchronized (pages) {
                pages.remove(cPage);
                nextFree--;
            }
        }
        ByteBuffer newBuffer = allocator.allocate(pageSize * multiplier, 1)[0];
        oldBuffer.position(0);
        if (multiplier < origMultiplier) {
            oldBuffer.limit(newBuffer.capacity());
        }
        newBuffer.put(oldBuffer);
        if (origMultiplier == 1) {
            largePages.getAndAdd(multiplier);
        } else if (multiplier == 1) {
            largePages.getAndAdd(-origMultiplier);
            pages.add(0, (VirtualPage) cPage);
            nextFree++;
        } else {
            largePages.getAndAdd(multiplier - origMultiplier);
        }
        ((VirtualPage) cPage).buffer = newBuffer;
        ((VirtualPage) cPage).multiplier = multiplier;
    }

    @Override
    public void unpin(ICachedPage page) throws HyracksDataException {
    }

    @Override
    public void flushDirtyPage(ICachedPage page) throws HyracksDataException {
    }

    @Override
    public void force(int fileId, boolean metadata) throws HyracksDataException {
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    @Override
    public int getPageSizeWithHeader() {
        return pageSize;
    }

    @Override
    public int getNumPages() {
        return numPages;
    }

    @Override
    public void open() throws HyracksDataException {
        if (open) {
            throw new HyracksDataException("Failed to open virtual buffercache since it is already open.");
        }
        pages.trimToSize();
        pages.ensureCapacity(numPages + OVERFLOW_PADDING);
        allocator.reserveAllocation(pageSize, numPages);
        for (int i = 0; i < numPages; i++) {
            buckets[i] = new CacheBucket();
        }
        nextFree = 0;
        largePages.set(0);
        open = true;
    }

    @Override
    public void reset() {
        for (int i = 0; i < numPages; i++) {
            buckets[i].cachedPage = null;
        }
        int excess = pages.size() - numPages;
        if (excess > 0) {
            for (int i = numPages + excess - 1; i >= numPages; i--) {
                pages.remove(i);
            }
        }
        nextFree = 0;
        largePages.set(0);
    }

    @Override
    public void close() throws HyracksDataException {
        if (!open) {
            throw new HyracksDataException("Failed to close virtual buffercache since it is already closed.");
        }

        pages.clear();
        for (int i = 0; i < numPages; i++) {
            buckets[i].cachedPage = null;
        }
        open = false;
    }

    public String dumpState() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Page size = %d\n", pageSize));
        sb.append(String.format("Capacity = %d\n", numPages));
        sb.append(String.format("Allocated pages = %d\n", pages.size()));
        sb.append(String.format("Allocated large pages = %d\n", largePages.get()));
        sb.append(String.format("Next free page = %d\n", nextFree));
        return sb.toString();
    }

    @Override
    public IFileMapManager getFileMapProvider() {
        return fileMapManager;
    }

    @Override
    public boolean isFull() {
        return (nextFree + largePages.get()) >= numPages;
    }

    private static class CacheBucket {
        private final ReentrantLock bucketLock;
        private VirtualPage cachedPage;

        public CacheBucket() {
            this.bucketLock = new ReentrantLock();
        }
    }

    private class VirtualPage implements ICachedPage {
        ByteBuffer buffer;
        final ReadWriteLock latch;
        volatile long dpid;
        int multiplier;
        VirtualPage next;

        public VirtualPage(ByteBuffer buffer) {
            this.buffer = buffer;
            latch = new ReentrantReadWriteLock(true);
            dpid = -1;
            next = null;
        }

        public void reset() {
            dpid = -1;
            next = null;
        }

        @Override
        public ByteBuffer getBuffer() {
            return buffer;
        }

        @Override
        public void acquireReadLatch() {
            latch.readLock().lock();
        }

        @Override
        public void releaseReadLatch() {
            latch.readLock().unlock();
        }

        @Override
        public void acquireWriteLatch() {
            latch.writeLock().lock();
        }

        @Override
        public void releaseWriteLatch(boolean markDirty) {
            latch.writeLock().unlock();
        }

        public boolean confiscated() {
            return false;
        }

        @Override
        public IQueueInfo getQueueInfo() {
            return null;
        }

        @Override
        public void setQueueInfo(IQueueInfo queueInfo) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getPageSize() {
            return pageSize;
        }

        @Override
        public int getFrameSizeMultiplier() {
            return multiplier;
        }
    }

    //These 4 methods aren't applicable here.
    @Override
    public int createMemFile() throws HyracksDataException {
        return 0;
    }

    @Override
    public void deleteMemFile(int fileId) throws HyracksDataException {
    }

    @Override
    public int getNumPagesOfFile(int fileId) throws HyracksDataException {
        return numPages;
    }

    @Override
    public void adviseWontNeed(ICachedPage page) {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.log(Level.INFO, "Calling adviseWontNeed on " + this.getClass().getName()
                    + " makes no sense as this BufferCache cannot evict pages");
        }
    }

    @Override
    public void returnPage(ICachedPage page) {

    }

    @Override
    public IFIFOPageQueue createFIFOQueue() {
        throw new UnsupportedOperationException("Virtual buffer caches don't have FIFO writers");
    }

    @Override
    public void finishQueue() {
        throw new UnsupportedOperationException("Virtual buffer caches don't have FIFO writers");
    }

    @Override
    public ICachedPage confiscatePage(long dpid) throws HyracksDataException {
        throw new UnsupportedOperationException("Virtual buffer caches don't have FIFO writers");
    }

    @Override
    public ICachedPage confiscateLargePage(long dpid, int multiplier, int extraBlockPageId)
            throws HyracksDataException {
        throw new UnsupportedOperationException("Virtual buffer caches don't have FIFO writers");
    }

    @Override
    public void copyPage(ICachedPage src, ICachedPage dst) {
        throw new UnsupportedOperationException("Virtual buffer caches don't have FIFO writers");
    }

    @Override
    public void setPageDiskId(ICachedPage page, long dpid) {

    }

    @Override
    public void returnPage(ICachedPage page, boolean reinsert) {
        throw new UnsupportedOperationException("Virtual buffer caches don't have FIFO writers");
    }

    @Override
    public int getFileReferenceCount(int fileId) {
        return 0;
    }

    @Override
    public boolean isReplicationEnabled() {
        return false;
    }

    @Override
    public IIOReplicationManager getIOReplicationManager() {
        return null;
    }

    @Override
    public void purgeHandle(int fileId) throws HyracksDataException {

    }

}
