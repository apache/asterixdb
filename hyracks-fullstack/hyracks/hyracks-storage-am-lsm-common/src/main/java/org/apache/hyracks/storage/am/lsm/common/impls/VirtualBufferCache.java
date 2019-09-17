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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.replication.IIOReplicationManager;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageWriter;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.IPageWriteFailureCallback;
import org.apache.hyracks.storage.common.buffercache.VirtualPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.storage.common.file.FileMapManager;
import org.apache.hyracks.storage.common.file.IFileMapManager;
import org.apache.hyracks.util.JSONUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class VirtualBufferCache implements IVirtualBufferCache {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final boolean DEBUG = true;
    private final ICacheMemoryAllocator allocator;
    private final IFileMapManager fileMapManager;
    private final int pageSize;
    private final int pageBudget;
    private final CacheBucket[] buckets;
    private final BlockingQueue<VirtualPage> freePages;
    private final AtomicInteger largePages;
    private final AtomicInteger used;
    private boolean open;

    public VirtualBufferCache(ICacheMemoryAllocator allocator, int pageSize, int pageBudget) {
        this.allocator = allocator;
        this.fileMapManager = new FileMapManager();
        this.pageSize = pageSize;
        if (pageBudget == 0) {
            throw new IllegalArgumentException("Page Budget Cannot be 0");
        }
        this.pageBudget = pageBudget;
        buckets = new CacheBucket[this.pageBudget];
        freePages = new ArrayBlockingQueue<>(this.pageBudget);
        largePages = new AtomicInteger(0);
        used = new AtomicInteger(0);
        open = false;
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    @Override
    public int getPageSizeWithHeader() {
        return pageSize;
    }

    public int getLargePages() {
        return largePages.get();
    }

    public int getUsage() {
        return used.get();
    }

    public int getPreAllocatedPages() {
        return freePages.size();
    }

    @Override
    public int getPageBudget() {
        return pageBudget;
    }

    @Override
    public boolean isFull() {
        return used.get() >= pageBudget;
    }

    @Override
    public int createFile(FileReference fileRef) throws HyracksDataException {
        synchronized (fileMapManager) {
            return fileMapManager.registerFile(fileRef);
        }
    }

    @Override
    public int openFile(FileReference fileRef) throws HyracksDataException {
        try {
            synchronized (fileMapManager) {
                if (fileMapManager.isMapped(fileRef)) {
                    return fileMapManager.lookupFileId(fileRef);
                }
                return fileMapManager.registerFile(fileRef);
            }
        } finally {
            logStats();
        }
    }

    private void logStats() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Free (allocated) pages = " + freePages.size() + ". Budget = " + pageBudget
                    + ". Large pages = " + largePages.get() + ". Overall usage = " + used.get());
        }
    }

    @Override
    public void openFile(int fileId) throws HyracksDataException {
        logStats();
    }

    @Override
    public void closeFile(int fileId) throws HyracksDataException {
    }

    @Override
    public void deleteFile(FileReference fileRef) throws HyracksDataException {
        synchronized (fileMapManager) {
            int fileId = fileMapManager.lookupFileId(fileRef);
            deleteFile(fileId);
        }
    }

    @Override
    public void deleteFile(int fileId) throws HyracksDataException {
        synchronized (fileMapManager) {
            fileMapManager.unregisterFile(fileId);
        }
        int reclaimedPages = 0;
        for (int i = 0; i < buckets.length; i++) {
            final CacheBucket bucket = buckets[i];
            bucket.bucketLock.lock();
            try {
                VirtualPage prev = null;
                VirtualPage curr = bucket.cachedPage;
                while (curr != null) {
                    if (BufferedFileHandle.getFileId(curr.dpid()) == fileId) {
                        reclaimedPages++;
                        if (curr.isLargePage()) {
                            largePages.getAndAdd(-curr.getFrameSizeMultiplier());
                            used.addAndGet(-curr.getFrameSizeMultiplier());
                        } else {
                            used.decrementAndGet();
                        }
                        if (prev == null) {
                            bucket.cachedPage = curr.next();
                            recycle(curr);
                            curr = bucket.cachedPage;
                        } else {
                            prev.next(curr.next());
                            recycle(curr);
                            curr = prev.next();
                        }
                    } else {
                        prev = curr;
                        curr = curr.next();
                    }
                }
            } finally {
                bucket.bucketLock.unlock();
            }
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Reclaimed pages = " + reclaimedPages);
        }
        logStats();
    }

    private void recycle(VirtualPage page) {
        // recycle only if
        // 1. not a large page
        // 2. allocation is not above budget
        if (DEBUG) {
            int readCount = page.getReadLatchCount();
            if (readCount > 0 || page.isWriteLatched()) {
                throw new IllegalStateException("Attempt to delete a file with latched pages (read: " + readCount
                        + ", write: " + page.isWriteLatched() + ")");
            }
        }
        if (used.get() < pageBudget && !page.isLargePage()) {
            page.reset();
            freePages.offer(page);
        }
    }

    @Override
    public ICachedPage pin(long dpid, boolean newPage) throws HyracksDataException {
        VirtualPage page;
        int hash = hash(dpid);
        CacheBucket bucket = buckets[hash];
        bucket.bucketLock.lock();
        try {
            page = bucket.cachedPage;
            while (page != null) {
                if (page.dpid() == dpid) {
                    return page;
                }
                page = page.next();
            }
            if (!newPage) {
                int fileId = BufferedFileHandle.getFileId(dpid);
                FileReference fileRef;
                synchronized (fileMapManager) {
                    fileRef = fileMapManager.lookupFileName(fileId);
                }
                throw HyracksDataException.create(ErrorCode.PAGE_DOES_NOT_EXIST_IN_FILE,
                        BufferedFileHandle.getPageId(dpid), fileRef);
            }
            page = getOrAllocPage(dpid);
            page.next(bucket.cachedPage);
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
        VirtualPage page = freePages.poll();
        if (page == null) {
            page = new VirtualPage(allocator.allocate(pageSize, 1)[0], pageSize);
            page.multiplier(1);
        }
        page.dpid(dpid);
        used.incrementAndGet();
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
        // Maintain counters
        // In addition, discard pre-allocated pages as the multiplier of the large page
        // This is done before actual resizing in order to allow GC for the same budget out of
        // the available free pages first
        if (origMultiplier == 1) {
            largePages.getAndAdd(multiplier);
            int diff = multiplier - 1;
            used.getAndAdd(diff);
            for (int i = 0; i < diff; i++) {
                freePages.poll();
            }
        } else if (multiplier == 1) {
            largePages.getAndAdd(-origMultiplier);
            used.addAndGet(-origMultiplier + 1);
        } else {
            int diff = multiplier - origMultiplier;
            largePages.getAndAdd(diff);
            used.getAndAdd(diff);
            for (int i = 0; i < diff; i++) {
                freePages.poll();
            }
        }
        ByteBuffer newBuffer = allocator.allocate(pageSize * multiplier, 1)[0];
        oldBuffer.position(0);
        if (multiplier < origMultiplier) {
            oldBuffer.limit(newBuffer.capacity());
        }
        newBuffer.put(oldBuffer);
        ((VirtualPage) cPage).buffer(newBuffer);
        ((VirtualPage) cPage).multiplier(multiplier);
    }

    @Override
    public void unpin(ICachedPage page) throws HyracksDataException {
    }

    @Override
    public void flush(ICachedPage page) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void force(int fileId, boolean metadata) throws HyracksDataException {
    }

    @Override
    public void open() throws HyracksDataException {
        if (open) {
            throw HyracksDataException.create(ErrorCode.VBC_ALREADY_OPEN);
        }
        allocator.reserveAllocation(pageSize, pageBudget);
        for (int i = 0; i < pageBudget; i++) {
            buckets[i] = new CacheBucket();
        }
        largePages.set(0);
        used.set(0);
        open = true;
    }

    @Override
    public void reset() {
        recycleAllPages();
        used.set(0);
        largePages.set(0);
    }

    private void recycleAllPages() {
        for (int i = 0; i < buckets.length; i++) {
            final CacheBucket bucket = buckets[i];
            bucket.bucketLock.lock();
            try {
                VirtualPage curr = bucket.cachedPage;
                while (curr != null) {
                    bucket.cachedPage = curr.next();
                    recycle(curr);
                    curr = bucket.cachedPage;
                }
            } finally {
                bucket.bucketLock.unlock();
            }
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (!open) {
            throw HyracksDataException.create(ErrorCode.VBC_ALREADY_CLOSED);
        }
        freePages.clear();
        for (int i = 0; i < pageBudget; i++) {
            buckets[i].cachedPage = null;
        }
        open = false;
    }

    public String dumpState() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Page size = %d%n", pageSize));
        sb.append(String.format("Page budget = %d%n", pageBudget));
        sb.append(String.format("Used pages = %d%n", used.get()));
        sb.append(String.format("Used large pages = %d%n", largePages.get()));
        sb.append(String.format("Available free pages = %d%n", freePages.size()));
        return sb.toString();
    }

    @Override
    public IFileMapManager getFileMapProvider() {
        return fileMapManager;
    }

    private static class CacheBucket {
        private final ReentrantLock bucketLock;
        private VirtualPage cachedPage;

        public CacheBucket() {
            this.bucketLock = new ReentrantLock();
        }

        @Override
        public String toString() {
            return CacheBucket.class.getSimpleName() + " -> " + (cachedPage == null ? "" : cachedPage.toString());
        }
    }

    @Override
    public int getNumPagesOfFile(int fileId) throws HyracksDataException {
        return -1;
    }

    @Override
    public void returnPage(ICachedPage page) {

    }

    @Override
    public IFIFOPageWriter createFIFOWriter(IPageWriteCallback callback, IPageWriteFailureCallback failureCallback) {
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
        deleteFile(fileId);
    }

    @Override
    public String toString() {
        return JSONUtil.fromMap(toMap());
    }

    private Map<String, Object> toMap() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("class", getClass().getSimpleName());
        map.put("allocator", allocator.toString());
        map.put("pageSize", pageSize);
        map.put("pageBudget", pageBudget);
        map.put("open", open);
        return map;
    }

    @Override
    public void closeFileIfOpen(FileReference fileRef) {
        throw new UnsupportedOperationException();
    }

}
