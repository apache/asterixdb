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
package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;
import edu.uci.ics.hyracks.storage.common.file.TransientFileMapManager;

public class VirtualBufferCache implements IVirtualBufferCache {
    private static final int OVERFLOW_PADDING = 8;

    private final ICacheMemoryAllocator allocator;
    private final IFileMapManager fileMapManager;
    private final int pageSize;
    private final int numPages;

    private final CacheBucket[] buckets;
    private final ArrayList<VirtualPage> pages;

    private volatile int nextFree;

    private boolean open;

    public VirtualBufferCache(ICacheMemoryAllocator allocator, int pageSize, int numPages) {
        this.allocator = allocator;
        this.fileMapManager = new TransientFileMapManager();
        this.pageSize = pageSize;
        this.numPages = numPages;

        buckets = new CacheBucket[numPages];
        pages = new ArrayList<VirtualPage>();
        nextFree = 0;
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
            CacheBucket bucket = buckets[i];
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
                throw new HyracksDataException("Page " + BufferedFileHandle.getPageId(dpid)
                        + " does not exist in file "
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
        return (int) (dpid % buckets.length);
    }

    private VirtualPage getOrAllocPage(long dpid) {
        VirtualPage page = null;
        synchronized (pages) {
            if (nextFree >= pages.size()) {
                page = new VirtualPage(allocator.allocate(pageSize, 1)[0]);
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
        ByteBuffer[] buffers = allocator.allocate(pageSize, numPages);
        for (int i = 0; i < numPages; i++) {
            pages.add(new VirtualPage(buffers[i]));
            buckets[i] = new CacheBucket();
        }
        nextFree = 0;
        open = true;
    }

    @Override
    public void reset() {
        for (int i = 0; i < numPages; i++) {
            pages.get(i).reset();
            buckets[i].cachedPage = null;
        }
        int excess = pages.size() - numPages;
        if (excess > 0) {
            for (int i = numPages + excess - 1; i >= numPages; i--) {
                pages.remove(i);
            }
        }
        nextFree = 0;
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
        sb.append(String.format("Next free page = %d\n", nextFree));
        return sb.toString();
    }

    @Override
    public IFileMapManager getFileMapProvider() {
        return fileMapManager;
    }

    @Override
    public boolean isFull() {
        return nextFree >= numPages;
    }

    private static class CacheBucket {
        private final ReentrantLock bucketLock;
        private VirtualPage cachedPage;

        public CacheBucket() {
            this.bucketLock = new ReentrantLock();
        }
    }

    private class VirtualPage implements ICachedPage {
        final ByteBuffer buffer;
        final ReadWriteLock latch;
        volatile long dpid;
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
        public void releaseWriteLatch() {
            latch.writeLock().unlock();
        }

    }
}
