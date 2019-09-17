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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;
import org.apache.hyracks.api.replication.IIOReplicationManager;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.storage.common.compression.file.ICompressedPageWriter;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.storage.common.file.IFileMapManager;
import org.apache.hyracks.util.IThreadStats;
import org.apache.hyracks.util.IThreadStatsCollector;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BufferCache implements IBufferCacheInternal, ILifeCycleComponent, IThreadStatsCollector {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int MAP_FACTOR = 3;

    private static final int MIN_CLEANED_COUNT_DIFF = 3;
    private static final int PIN_MAX_WAIT_TIME = 50;
    private static final int PIN_ATTEMPT_CYCLES_WARNING_THRESHOLD = 3;
    private static final int MAX_PIN_ATTEMPT_CYCLES = 1000;
    private static final int MAX_PAGE_READ_ATTEMPTS = 5;
    private static final long PERIOD_BETWEEN_READ_ATTEMPTS = 100;
    public static final boolean DEBUG = false;

    private final int pageSize;
    private final int maxOpenFiles;
    final IIOManager ioManager;
    private final CacheBucket[] pageMap;
    private final IPageReplacementStrategy pageReplacementStrategy;
    private final IPageCleanerPolicy pageCleanerPolicy;
    private final IFileMapManager fileMapManager;
    private final CleanerThread cleanerThread;
    private final Map<Integer, BufferedFileHandle> fileInfoMap;
    private final BlockingQueue<BufferCacheHeaderHelper> headerPageCache;

    private IIOReplicationManager ioReplicationManager;
    private final List<ICachedPageInternal> cachedPages = new ArrayList<>();
    private final AtomicLong masterPinCount = new AtomicLong();
    private final Map<Long, IThreadStats> statsSubscribers = new ConcurrentHashMap<>();

    private boolean closed;

    //DEBUG
    private static final Level fileOpsLevel = Level.TRACE;
    private ArrayList<CachedPage> confiscatedPages;
    private Lock confiscateLock;
    private HashMap<CachedPage, StackTraceElement[]> confiscatedPagesOwner;
    private ConcurrentHashMap<CachedPage, StackTraceElement[]> pinnedPageOwner;

    public BufferCache(IIOManager ioManager, IPageReplacementStrategy pageReplacementStrategy,
            IPageCleanerPolicy pageCleanerPolicy, IFileMapManager fileMapManager, int maxOpenFiles, int ioQueuelen,
            ThreadFactory threadFactory) {
        this.headerPageCache = new ArrayBlockingQueue<>(ioQueuelen);
        this.ioManager = ioManager;
        this.pageSize = pageReplacementStrategy.getPageSize();
        this.maxOpenFiles = maxOpenFiles;
        pageReplacementStrategy.setBufferCache(this);
        pageMap = new CacheBucket[pageReplacementStrategy.getMaxAllowedNumPages() * MAP_FACTOR + 1];
        for (int i = 0; i < pageMap.length; ++i) {
            pageMap[i] = new CacheBucket();
        }
        this.pageReplacementStrategy = pageReplacementStrategy;
        this.pageCleanerPolicy = pageCleanerPolicy;
        this.fileMapManager = fileMapManager;

        Executor executor = Executors.newCachedThreadPool(threadFactory);
        fileInfoMap = new HashMap<>();
        cleanerThread = new CleanerThread();
        executor.execute(cleanerThread);
        closed = false;

        if (DEBUG) {
            confiscatedPages = new ArrayList<>();
            confiscatedPagesOwner = new HashMap<>();
            confiscateLock = new ReentrantLock();
            pinnedPageOwner = new ConcurrentHashMap<>();
        }
    }

    //this constructor is used when replication is enabled to pass the IIOReplicationManager
    public BufferCache(IIOManager ioManager, IPageReplacementStrategy pageReplacementStrategy,
            IPageCleanerPolicy pageCleanerPolicy, IFileMapManager fileMapManager, int maxOpenFiles, int ioQueueLen,
            ThreadFactory threadFactory, IIOReplicationManager ioReplicationManager) {

        this(ioManager, pageReplacementStrategy, pageCleanerPolicy, fileMapManager, maxOpenFiles, ioQueueLen,
                threadFactory);
        this.ioReplicationManager = ioReplicationManager;
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    @Override
    public int getPageSizeWithHeader() {
        return pageSize + RESERVED_HEADER_BYTES;
    }

    @Override
    public int getPageBudget() {
        return pageReplacementStrategy.getMaxAllowedNumPages();
    }

    private void pinSanityCheck(long dpid) throws HyracksDataException {
        if (closed) {
            throw new HyracksDataException("pin called on a closed cache");
        }

        // check whether file has been created and opened
        int fileId = BufferedFileHandle.getFileId(dpid);
        BufferedFileHandle fInfo;
        synchronized (fileInfoMap) {
            fInfo = fileInfoMap.get(fileId);
        }
        if (fInfo == null || fInfo.hasBeenDeleted() || !fInfo.hasBeenOpened()) {
            throw new HyracksDataException("pin called on a fileId " + fileId + " that has not been created.");
        } else if (fInfo.getReferenceCount() <= 0) {
            throw new HyracksDataException("pin called on a fileId " + fileId + " that has not been opened.");
        }
    }

    @Override
    public ICachedPage pin(long dpid, boolean newPage) throws HyracksDataException {
        // Calling the pinSanityCheck should be used only for debugging, since
        // the synchronized block over the fileInfoMap is a hot spot.
        if (DEBUG) {
            pinSanityCheck(dpid);
        }
        final IThreadStats threadStats = statsSubscribers.get(Thread.currentThread().getId());
        if (threadStats != null) {
            threadStats.pagePinned();
        }
        CachedPage cPage = findPage(dpid);
        if (!newPage) {
            if (DEBUG) {
                confiscateLock.lock();
                try {
                    for (CachedPage c : confiscatedPages) {
                        if (c.dpid == dpid && c.confiscated.get()) {
                            throw new IllegalStateException();
                        }
                    }
                } finally {
                    confiscateLock.unlock();
                }
            }
            // Resolve race of multiple threads trying to read the page from
            // disk.
            synchronized (cPage) {
                if (!cPage.valid) {
                    try {
                        tryRead(cPage);
                        cPage.valid = true;
                    } catch (Exception e) {
                        LOGGER.log(ExceptionUtils.causedByInterrupt(e) ? Level.DEBUG : Level.WARN,
                                "Failure while trying to read a page from disk", e);
                        throw e;
                    } finally {
                        if (!cPage.valid) {
                            unpin(cPage);
                        }
                    }
                }
            }
        } else {
            cPage.valid = true;
        }
        pageReplacementStrategy.notifyCachePageAccess(cPage);
        if (DEBUG) {
            pinnedPageOwner.put(cPage, Thread.currentThread().getStackTrace());
        }
        return cPage;
    }

    private CachedPage findPage(long dpid) throws HyracksDataException {
        return (CachedPage) getPageLoop(dpid, -1, false);
    }

    private ICachedPage findPageInner(long dpid) {
        CachedPage cPage;
        /*
         * Hash dpid to get a bucket and then check if the page exists in
         * the bucket.
         */
        int hash = hash(dpid);
        CacheBucket bucket = pageMap[hash];
        bucket.bucketLock.lock();
        try {
            cPage = bucket.cachedPage;
            while (cPage != null) {
                if (DEBUG) {
                    assert bucket.cachedPage != bucket.cachedPage.next;
                }
                if (cPage.dpid == dpid) {
                    if (DEBUG) {
                        assert !cPage.confiscated.get();
                    }
                    cPage.pinCount.incrementAndGet();
                    return cPage;
                }
                cPage = cPage.next;
            }
        } finally {
            bucket.bucketLock.unlock();
        }
        /*
         * If we got here, the page was not in the hash table. Now we ask
         * the page replacement strategy to find us a victim.
         */
        CachedPage victim = (CachedPage) pageReplacementStrategy.findVictim();
        if (victim == null) {
            return null;
        }
        /*
         * We have a victim with the following invariants. 1. The dpid
         * on the CachedPage may or may not be valid. 2. We have a pin
         * on the CachedPage. We have to deal with three cases here.
         * Case 1: The dpid on the CachedPage is invalid (-1). This
         * indicates that this buffer has never been used or is a
         * confiscated page. So we are the only ones holding it. Get a lock
         * on the required dpid's hash bucket, check if someone inserted
         * the page we want into the table. If so, decrement the
         * pincount on the victim and return the winner page in the
         * table. If such a winner does not exist, insert the victim and
         * return it. Case 2: The dpid on the CachedPage is valid. Case
         * 2a: The current dpid and required dpid hash to the same
         * bucket. Get the bucket lock, check that the victim is still
         * at pinCount == 1 If so check if there is a winning CachedPage
         * with the required dpid. If so, decrement the pinCount on the
         * victim and return the winner. If not, update the contents of
         * the CachedPage to hold the required dpid and return it. If
         * the picCount on the victim was != 1 or CachedPage was dirty
         * someone used the victim for its old contents -- Decrement the
         * pinCount and retry. Case 2b: The current dpid and required
         * dpid hash to different buckets. Get the two bucket locks in
         * the order of the bucket indexes (Ordering prevents
         * deadlocks). Check for the existence of a winner in the new
         * bucket and for potential use of the victim (pinCount != 1).
         * If everything looks good, remove the CachedPage from the old
         * bucket, and add it to the new bucket and update its header
         * with the new dpid.
         */
        if (victim.dpid < 0) {
            /*
             * Case 1.
             */
            bucket.bucketLock.lock();
            try {
                if (!victim.pinCount.compareAndSet(0, 1)) {
                    return null;
                }
                // now that we have the pin, ensure the victim's dpid still is < 0, if it's not, decrement
                // pin count and try again
                if (victim.dpid >= 0) {
                    victim.pinCount.decrementAndGet();
                    return null;
                }
                if (DEBUG) {
                    confiscateLock.lock();
                    try {
                        if (confiscatedPages.contains(victim)) {
                            throw new IllegalStateException();
                        }
                    } finally {
                        confiscateLock.unlock();
                    }
                }
                cPage = findTargetInBucket(dpid, bucket.cachedPage, victim);
                if (cPage != null) {
                    return cPage;
                }
                victim.reset(dpid);
                victim.next = bucket.cachedPage;
                bucket.cachedPage = victim;
            } finally {
                bucket.bucketLock.unlock();
            }

            if (DEBUG) {
                assert !victim.confiscated.get();
            }
            return victim;
        }
        int victimHash = hash(victim.dpid);
        if (victimHash == hash) {
            /*
             * Case 2a.
             */
            bucket.bucketLock.lock();
            try {
                if (!victim.pinCount.compareAndSet(0, 1)) {
                    return null;
                }
                // now that we have the pin, ensure the victim's bucket hasn't changed, if it has, decrement
                // pin count and try again
                if (victimHash != hash(victim.dpid)) {
                    victim.pinCount.decrementAndGet();
                    return null;
                }
                if (DEBUG) {
                    confiscateLock.lock();
                    try {
                        if (confiscatedPages.contains(victim)) {
                            throw new IllegalStateException();
                        }
                    } finally {
                        confiscateLock.unlock();
                    }
                }
                cPage = findTargetInBucket(dpid, bucket.cachedPage, victim);
                if (cPage != null) {
                    return cPage;
                }
                victim.reset(dpid);
            } finally {
                bucket.bucketLock.unlock();
            }
            if (DEBUG) {
                assert !victim.confiscated.get();
            }
            return victim;
        } else {
            /*
             * Case 2b.
             */
            CacheBucket victimBucket = pageMap[victimHash];
            if (victimHash < hash) {
                victimBucket.bucketLock.lock();
                bucket.bucketLock.lock();
            } else {
                bucket.bucketLock.lock();
                victimBucket.bucketLock.lock();
            }
            try {
                if (!victim.pinCount.compareAndSet(0, 1)) {
                    return null;
                }
                // now that we have the pin, ensure the victim's bucket hasn't changed, if it has, decrement
                // pin count and try again
                if (victimHash != hash(victim.dpid)) {
                    victim.pinCount.decrementAndGet();
                    return null;
                }
                if (DEBUG && confiscatedPages.contains(victim)) {
                    throw new IllegalStateException();
                }
                cPage = findTargetInBucket(dpid, bucket.cachedPage, victim);
                if (cPage != null) {
                    return cPage;
                }
                if (victimBucket.cachedPage == victim) {
                    victimBucket.cachedPage = victim.next;
                } else {
                    CachedPage victimPrev = victimBucket.cachedPage;
                    while (victimPrev.next != victim) {
                        victimPrev = victimPrev.next;
                        if (victimPrev == null) {
                            throw new IllegalStateException();
                        }
                    }
                    victimPrev.next = victim.next;
                }
                victim.reset(dpid);
                victim.next = bucket.cachedPage;
                bucket.cachedPage = victim;
            } finally {
                victimBucket.bucketLock.unlock();
                bucket.bucketLock.unlock();
            }
            if (DEBUG) {
                assert !victim.confiscated.get();
            }
            return victim;
        }
    }

    private CachedPage findTargetInBucket(long dpid, CachedPage cPage, CachedPage victim) {
        while (cPage != null) {
            if (cPage.dpid == dpid) {
                cPage.pinCount.incrementAndGet();
                victim.pinCount.decrementAndGet();
                if (DEBUG) {
                    assert !cPage.confiscated.get();
                }
                break;
            }
            cPage = cPage.next;
        }
        return cPage;
    }

    private String dumpState() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("Buffer cache state\n");
        buffer.append("Page Size: ").append(pageSize).append('\n');
        buffer.append("Number of physical pages: ").append(pageReplacementStrategy.getMaxAllowedNumPages())
                .append('\n');
        buffer.append("Hash table size: ").append(pageMap.length).append('\n');
        buffer.append("Page Map:\n");
        buffer.append("cpid -> [fileId:pageId, pinCount, valid/invalid, confiscated/physical, dirty/clean]");
        int nCachedPages = 0;
        for (int i = 0; i < pageMap.length; ++i) {
            final CacheBucket cb = pageMap[i];
            cb.bucketLock.lock();
            try {
                CachedPage cp = cb.cachedPage;
                if (cp == null) {
                    continue;
                }
                buffer.append("   ").append(i).append('\n');
                while (cp != null) {
                    buffer.append("      ").append(cp.cpid).append(" -> [")
                            .append(BufferedFileHandle.getFileId(cp.dpid)).append(':')
                            .append(BufferedFileHandle.getPageId(cp.dpid)).append(", ").append(cp.pinCount.get())
                            .append(", ").append(cp.valid ? "valid" : "invalid").append(", ")
                            .append(cp.confiscated.get() ? "confiscated" : "physical").append(", ")
                            .append(cp.dirty.get() ? "dirty" : "clean").append("]\n");
                    cp = cp.next;
                    ++nCachedPages;
                }
            } finally {
                cb.bucketLock.unlock();
            }
        }
        buffer.append("Number of cached pages: ").append(nCachedPages).append('\n');
        if (DEBUG) {
            confiscateLock.lock();
            try {
                buffer.append("Number of confiscated pages: ").append(confiscatedPages.size()).append('\n');
            } finally {
                confiscateLock.unlock();
            }
        }
        return buffer.toString();
    }

    @Override
    public boolean isClean() {
        List<Long> reachableDpids = new LinkedList<>();
        synchronized (cachedPages) {
            for (ICachedPageInternal internalPage : cachedPages) {
                CachedPage c = (CachedPage) internalPage;
                if (c.confiscated() || c.latch.getReadLockCount() != 0 || c.latch.getWriteHoldCount() != 0) {
                    return false;
                }
                if (c.valid) {
                    reachableDpids.add(c.dpid);
                }
            }
        }
        for (Long l : reachableDpids) {
            if (!canFindValidCachedPage(l)) {
                return false;
            }
        }
        return true;
    }

    private boolean canFindValidCachedPage(long dpid) {
        int hash = hash(dpid);
        CachedPage cPage = null;
        CacheBucket bucket = pageMap[hash];
        bucket.bucketLock.lock();
        try {
            cPage = bucket.cachedPage;
            while (cPage != null) {
                assert bucket.cachedPage != bucket.cachedPage.next;
                if (cPage.dpid == dpid) {
                    return true;
                }
                cPage = cPage.next;
            }
        } finally {
            bucket.bucketLock.unlock();
        }
        return false;
    }

    private void tryRead(CachedPage cPage) throws HyracksDataException {
        for (int i = 1; i <= MAX_PAGE_READ_ATTEMPTS; i++) {
            try {
                read(cPage);
                return;
            } catch (HyracksDataException readException) {
                if (readException.getErrorCode() == ErrorCode.CANNOT_READ_CLOSED_FILE && i <= MAX_PAGE_READ_ATTEMPTS) {
                    /**
                     * if the read failure was due to another thread closing the file channel because
                     * it was interrupted, we will try to read again since the interrupted thread
                     * will re-open the file.
                     */
                    try {
                        Thread.sleep(PERIOD_BETWEEN_READ_ATTEMPTS);
                        LOGGER.log(Level.WARN, String.format("Failed to read page. Retrying attempt (%d/%d)", i + 1,
                                MAX_PAGE_READ_ATTEMPTS), readException);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw HyracksDataException.create(e);
                    }
                } else {
                    throw readException;
                }
            }
        }
    }

    private void read(CachedPage cPage) throws HyracksDataException {
        BufferedFileHandle fInfo = getFileHandle(cPage);
        cPage.buffer.clear();
        fInfo.read(cPage);
    }

    @Override
    public void resizePage(ICachedPage cPage, int totalPages, IExtraPageBlockHelper extraPageBlockHelper)
            throws HyracksDataException {
        pageReplacementStrategy.resizePage((ICachedPageInternal) cPage, totalPages, extraPageBlockHelper);
    }

    void write(CachedPage cPage) throws HyracksDataException {
        BufferedFileHandle fInfo = getFileHandle(cPage);
        // synchronize on fInfo to prevent the file handle from being deleted until the page is written.
        synchronized (fInfo) {
            if (fInfo.hasBeenDeleted()) {
                return;
            }
            fInfo.write(cPage);
        }

    }

    @Override
    public void unpin(ICachedPage page) throws HyracksDataException {
        if (closed) {
            throw new HyracksDataException("unpin called on a closed cache");
        }
        int pinCount = ((CachedPage) page).pinCount.decrementAndGet();
        if (DEBUG && pinCount == 0) {
            pinnedPageOwner.remove(page);
        }
    }

    @Override
    public void subscribe(IThreadStats stats) {
        statsSubscribers.put(Thread.currentThread().getId(), stats);
    }

    @Override
    public void unsubscribe() {
        statsSubscribers.remove(Thread.currentThread().getId());
    }

    private int hash(long dpid) {
        int hashValue = (int) dpid ^ (Integer.reverse((int) (dpid >>> 32)) >>> 1);
        return hashValue % pageMap.length;
    }

    private static class CacheBucket {
        private final Lock bucketLock;
        private CachedPage cachedPage;

        public CacheBucket() {
            bucketLock = new ReentrantLock();
        }
    }

    @Override
    public ICachedPageInternal getPage(int cpid) {
        synchronized (cachedPages) {
            return cachedPages.get(cpid);
        }
    }

    private class CleanerThread implements Runnable {
        private volatile boolean shutdownStart = false;
        private volatile boolean shutdownComplete = false;
        private final Object threadLock = new Object();
        private final Object cleanNotification = new Object();
        // Simply keeps incrementing this counter when a page is cleaned.
        // Used to implement wait-for-cleanerthread heuristic optimizations.
        // A waiter can detect whether pages have been cleaned.
        private volatile int cleanedCount = 0;

        public void cleanPage(CachedPage cPage, boolean force) {
            if (cPage.dirty.get() && !cPage.confiscated.get()) {
                boolean proceed = false;
                if (force) {
                    cPage.latch.writeLock().lock();
                    proceed = true;
                } else {
                    proceed = cPage.latch.readLock().tryLock();
                }
                if (proceed) {
                    try {
                        cleanPageLocked(cPage);
                    } finally {
                        if (force) {
                            cPage.latch.writeLock().unlock();
                        } else {
                            cPage.latch.readLock().unlock();
                        }
                    }
                } else if (shutdownStart) {
                    throw new IllegalStateException(
                            "Cache closed, but unable to acquire read lock on dirty page: " + cPage.dpid);
                }
            }
        }

        private void cleanPageLocked(CachedPage cPage) {
            // Make sure page is still dirty.
            if (!cPage.dirty.get()) {
                return;
            }
            boolean cleaned = true;
            try {
                write(cPage);
            } catch (HyracksDataException e) {
                LOGGER.log(Level.WARN, "Unable to write dirty page", e);
                cleaned = false;
            }
            if (cleaned) {
                cPage.dirty.set(false);
                cPage.pinCount.decrementAndGet();
                // this increment of a volatile is OK as there is only one writer
                cleanedCount++;
                synchronized (cleanNotification) {
                    cleanNotification.notifyAll();
                }
            }
        }

        @Override
        public void run() {
            synchronized (threadLock) {
                try {
                    while (!shutdownStart) {
                        runCleanCycle();
                    }
                } catch (InterruptedException e) {

                    Thread.currentThread().interrupt();
                } finally {
                    shutdownComplete = true;
                    threadLock.notifyAll();
                }
            }
        }

        private void runCleanCycle() throws InterruptedException {
            pageCleanerPolicy.notifyCleanCycleStart(threadLock);
            int curPage = 0;
            while (true) {
                synchronized (cachedPages) {
                    if (curPage >= cachedPages.size()) {
                        break;
                    }
                    CachedPage cPage = (CachedPage) cachedPages.get(curPage);
                    if (cPage != null) {
                        cleanPage(cPage, false);
                    }
                }
                curPage++;
            }
            if (!shutdownStart) {
                pageCleanerPolicy.notifyCleanCycleFinish(threadLock);
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            synchronized (cleanerThread.threadLock) {
                cleanerThread.shutdownStart = true;
                cleanerThread.threadLock.notifyAll();
                while (!cleanerThread.shutdownComplete) {
                    cleanerThread.threadLock.wait();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        synchronized (fileInfoMap) {
            fileInfoMap.forEach((key, value) -> {
                try {
                    sweepAndFlush(value, true);
                    value.close();
                } catch (HyracksDataException e) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.log(Level.WARN, "Error flushing file id: " + key, e);
                    }
                }
            });
            fileInfoMap.clear();
        }
    }

    @Override
    public int createFile(FileReference fileRef) throws HyracksDataException {
        if (LOGGER.isEnabled(fileOpsLevel)) {
            LOGGER.log(fileOpsLevel, "Creating file: " + fileRef + " in cache: " + this);
        }
        BufferedFileHandle.createFile(this, fileRef);
        int fileId;
        try {
            synchronized (fileInfoMap) {
                fileId = fileMapManager.registerFile(fileRef);
                getOrCreateFileHandle(fileId);
            }
            return fileId;
        } catch (Exception e) {
            // If file registration failed for any reason, we need to undo the file creation
            try {
                IoUtil.delete(fileRef);
            } catch (Exception deleteException) {
                e.addSuppressed(deleteException);
            }
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public int openFile(FileReference fileRef) throws HyracksDataException {
        if (LOGGER.isEnabled(fileOpsLevel)) {
            LOGGER.log(fileOpsLevel, "Opening file: " + fileRef + " in cache: " + this);
        }
        int fileId = -1;
        synchronized (fileInfoMap) {
            if (fileMapManager.isMapped(fileRef)) {
                fileId = fileMapManager.lookupFileId(fileRef);
            } else {
                fileId = fileMapManager.registerFile(fileRef);
            }
        }
        openFile(fileId);
        return fileId;
    }

    @Override
    public void openFile(int fileId) throws HyracksDataException {
        if (LOGGER.isEnabled(fileOpsLevel)) {
            LOGGER.log(fileOpsLevel, "Opening file: " + fileId + " in cache: " + this);
        }
        try {
            final BufferedFileHandle fInfo = getOrCreateFileHandle(fileId);
            //CompressedFileReference may open another file which may sweep and close out this fInfo
            fInfo.incReferenceCount();

            if (!fInfo.hasBeenOpened()) {
                // a new file
                synchronized (fInfo) {
                    // prevent concurrent opening of the same file
                    if (!fInfo.hasBeenOpened()) {
                        if (fileInfoMap.size() > maxOpenFiles) {
                            closeOpeningFiles(fInfo);
                        }
                        // create, open, and map new file reference
                        FileReference fileRef;
                        synchronized (fileInfoMap) {
                            fileRef = fileMapManager.lookupFileName(fileId);
                        }
                        fInfo.open(fileRef);
                    }
                }
            }
        } catch (Exception e) {
            removeFileHandle(fileId);
            throw HyracksDataException.create(e);
        }
    }

    private void closeOpeningFiles(BufferedFileHandle newFileHandle) throws HyracksDataException {
        synchronized (fileInfoMap) {
            boolean unreferencedFileFound = true;
            while (fileInfoMap.size() > maxOpenFiles && unreferencedFileFound) {
                // map is full, make room by cleaning up unreferenced files
                unreferencedFileFound = false;
                for (Map.Entry<Integer, BufferedFileHandle> entry : fileInfoMap.entrySet()) {
                    BufferedFileHandle fh = entry.getValue();
                    if (fh != newFileHandle && fh.getReferenceCount() <= 0) {
                        if (fh.getReferenceCount() < 0) {
                            throw new IllegalStateException("Illegal reference count " + fh.getReferenceCount()
                                    + " of file " + fh.getFileReference());
                        }
                        int entryFileId = entry.getKey();
                        sweepAndFlush(fh, true);
                        entry.getValue().close();
                        fileInfoMap.remove(entryFileId);
                        unreferencedFileFound = true;
                        // for-each iterator is invalid because we changed
                        // fileInfoMap
                        break;
                    }
                }
            }
            if (fileInfoMap.size() > maxOpenFiles) {
                throw new HyracksDataException("Could not open fileId " + newFileHandle.getFileId()
                        + ". Max number of files " + maxOpenFiles + " already opened and referenced.");
            }
        }
    }

    private void sweepAndFlush(BufferedFileHandle fInfo, boolean flushDirtyPages) throws HyracksDataException {
        if (!fInfo.hasBeenOpened()) {
            //Skip flushing as the file has not been open
            return;
        }
        final int fileId = fInfo.getFileId();
        for (final CacheBucket bucket : pageMap) {
            bucket.bucketLock.lock();
            try {
                CachedPage prev = bucket.cachedPage;
                while (prev != null) {
                    CachedPage cPage = prev.next;
                    if (cPage == null) {
                        break;
                    }
                    if (invalidateIfFileIdMatch(fileId, cPage, flushDirtyPages)) {
                        prev.next = cPage.next;
                        cPage.next = null;
                    } else {
                        prev = cPage;
                    }
                }
                // Take care of the head of the chain.
                if (bucket.cachedPage != null && invalidateIfFileIdMatch(fileId, bucket.cachedPage, flushDirtyPages)) {
                    CachedPage cPage = bucket.cachedPage;
                    bucket.cachedPage = bucket.cachedPage.next;
                    cPage.next = null;
                }
            } finally {
                bucket.bucketLock.unlock();
            }
        }
    }

    private boolean invalidateIfFileIdMatch(int fileId, CachedPage cPage, boolean flushDirtyPages)
            throws HyracksDataException {
        if (BufferedFileHandle.getFileId(cPage.dpid) == fileId) {
            int pinCount;
            if (cPage.dirty.get()) {
                if (flushDirtyPages) {
                    write(cPage);
                }
                cPage.dirty.set(false);
                pinCount = cPage.pinCount.decrementAndGet();
            } else {
                pinCount = cPage.pinCount.get();
            }
            if (pinCount > 0) {
                throw new IllegalStateException("Page " + BufferedFileHandle.getFileId(cPage.dpid) + ":"
                        + BufferedFileHandle.getPageId(cPage.dpid)
                        + " is pinned and file is being closed. Pincount is: " + pinCount + " Page is confiscated: "
                        + cPage.confiscated);
            }
            cPage.invalidate();
            return true;
        }
        return false;
    }

    @Override
    public void closeFile(int fileId) throws HyracksDataException {
        if (LOGGER.isEnabled(fileOpsLevel)) {
            LOGGER.log(fileOpsLevel, "Closing file: " + fileId + " in cache: " + this);
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(dumpState());
        }

        synchronized (fileInfoMap) {
            BufferedFileHandle fInfo = fileInfoMap.get(fileId);
            if (fInfo == null || !fInfo.hasBeenOpened()) {
                throw new HyracksDataException("Closing unopened file");
            }
            if (fInfo.decReferenceCount() < 0) {
                throw new HyracksDataException("Closed fileId: " + fileId + " more times than it was opened.");
            }
        }
        if (LOGGER.isEnabled(fileOpsLevel)) {
            LOGGER.log(fileOpsLevel, "Closed file: " + fileId + " in cache: " + this);
        }
    }

    @Override
    public void flush(ICachedPage page) throws HyracksDataException {
        // Assumes the caller has pinned the page.
        cleanerThread.cleanPage((CachedPage) page, true);
    }

    @Override
    public void force(int fileId, boolean metadata) throws HyracksDataException {
        BufferedFileHandle fInfo;
        synchronized (fileInfoMap) {
            fInfo = fileInfoMap.get(fileId);
        }
        fInfo.force(metadata);
    }

    @Override
    public void deleteFile(FileReference fileRef) throws HyracksDataException {
        boolean mapped = false;
        int fileId = -1;
        synchronized (fileInfoMap) {
            if (fileMapManager.isMapped(fileRef)) {
                mapped = true;
                fileId = fileMapManager.lookupFileId(fileRef);
            }
        }
        if (mapped) {
            deleteFile(fileId);
        } else {
            BufferedFileHandle.deleteFile(fileRef);
        }
    }

    @Override
    public void deleteFile(int fileId) throws HyracksDataException {
        if (LOGGER.isEnabled(fileOpsLevel)) {
            LOGGER.log(fileOpsLevel, "Deleting file: " + fileId + " in cache: " + this);
        }
        BufferedFileHandle fInfo = removeFileHandle(fileId);
        if (fInfo == null) {
            return;
        }
        sweepAndFlush(fInfo, false);
        try {
            if (fInfo.getReferenceCount() > 0) {
                throw new HyracksDataException("Deleting open file");
            }
        } finally {
            FileReference fileRef = null;
            try {
                synchronized (fileInfoMap) {
                    fileRef = fileMapManager.unregisterFile(fileId);
                }
            } finally {
                try {
                    synchronized (fInfo) {
                        fInfo.close();
                        fInfo.markAsDeleted();
                    }
                } finally {
                    BufferedFileHandle.deleteFile(fileRef);
                }
            }
        }
    }

    @Override
    public synchronized int getFileReferenceCount(int fileId) {
        synchronized (fileInfoMap) {
            BufferedFileHandle fInfo = fileInfoMap.get(fileId);
            if (fInfo != null) {
                return fInfo.getReferenceCount();
            } else {
                return 0;
            }
        }
    }

    @Override
    public void start() {
        // no op
    }

    @Override
    public void stop(boolean dumpState, OutputStream os) throws IOException {
        if (dumpState) {
            dumpState(os);
        }
        close();
    }

    @Override
    public void addPage(ICachedPageInternal page) {
        synchronized (cachedPages) {
            final int cpid = page.getCachedPageId();
            if (cpid < cachedPages.size()) {
                cachedPages.set(cpid, page);
            } else {
                if (cpid > cachedPages.size()) {
                    // 4 > 1 -> [exiting, null, null, null, new]
                    cachedPages.addAll(Collections.nCopies(cpid - cachedPages.size(), null));
                }
                cachedPages.add(page);
            }
        }
    }

    @Override
    public boolean removePage(ICachedPageInternal victimPage) {
        CachedPage victim = (CachedPage) victimPage;
        // Case 1 from findPage()
        if (victim.dpid < 0) { // new page
            if (!victim.pinCount.compareAndSet(0, 1)) {
                return false;
            }
            // now that we have the pin, ensure the victim's dpid still is < 0, if it's not, decrement
            // pin count and try again
            if (victim.dpid >= 0) {
                victim.pinCount.decrementAndGet();
                return false;
            }
        } else {
            // Case 2a/b
            int pageHash = hash(victim.dpid);
            CacheBucket bucket = pageMap[pageHash];
            bucket.bucketLock.lock();
            try {
                if (!victim.pinCount.compareAndSet(0, 1)) {
                    return false;
                }
                // now that we have the pin, ensure the victim's bucket hasn't changed, if it has, decrement
                // pin count and try again
                if (pageHash != hash(victim.dpid)) {
                    victim.pinCount.decrementAndGet();
                    return false;
                }
                // readjust the next pointers to remove this page from
                // the pagemap
                CachedPage curr = bucket.cachedPage;
                CachedPage prev = null;
                boolean found = false;
                //traverse the bucket's linked list to find the victim.
                while (curr != null) {
                    if (curr == victim) {
                        // we found where the victim
                        // resides in the hash table
                        if (DEBUG) {
                            assert curr != curr.next;
                        }
                        if (prev == null) {
                            // if this is the first page in the bucket
                            bucket.cachedPage = curr.next;
                        } else {
                            // if it isn't we need to make the previous
                            // node point to where it should
                            prev.next = curr.next;
                            if (DEBUG) {
                                assert prev.next != prev;
                            }
                        }
                        curr.next = null;
                        found = true;
                        break;
                    }
                    // go to the next entry
                    prev = curr;
                    curr = curr.next;
                }
                assert found;
            } finally {
                bucket.bucketLock.unlock();
            }
        }
        synchronized (cachedPages) {
            ICachedPageInternal old = cachedPages.set(victim.cpid, null);
            if (DEBUG) {
                assert old == victim;
            }
        }
        return true;
    }

    @Override
    public void dumpState(OutputStream os) throws IOException {
        os.write(dumpState().getBytes());
    }

    @Override
    public int getNumPagesOfFile(int fileId) throws HyracksDataException {
        synchronized (fileInfoMap) {
            BufferedFileHandle fInfo = fileInfoMap.get(fileId);
            if (fInfo == null) {
                throw new HyracksDataException("No such file mapped for fileId:" + fileId);
            }
            return fInfo.getNumberOfPages();
        }
    }

    @Override
    public ICachedPage confiscatePage(long dpid) throws HyracksDataException {
        return confiscatePage(dpid, 1);
    }

    @Override
    public ICachedPage confiscateLargePage(long dpid, int multiplier, int extraBlockPageId)
            throws HyracksDataException {
        ICachedPage cachedPage = confiscatePage(dpid, multiplier);
        ((ICachedPageInternal) cachedPage).setExtraBlockPageId(extraBlockPageId);
        return cachedPage;
    }

    private ICachedPage confiscatePage(long dpid, int multiplier) throws HyracksDataException {
        return getPageLoop(dpid, multiplier, true);
    }

    private ICachedPage confiscateInner(long dpid, int multiplier) {
        ICachedPage returnPage = null;
        CachedPage victim = (CachedPage) pageReplacementStrategy.findVictim(multiplier);
        if (victim == null) {
            return victim;
        }
        if (DEBUG) {
            assert !victim.confiscated.get();
        }
        // find a page that would possibly be evicted anyway
        // Case 1 from findPage()
        if (victim.dpid < 0) { // new page
            if (!victim.pinCount.compareAndSet(0, 1)) {
                return null;
            }
            // now that we have the pin, ensure the victim's dpid still is < 0, if it's not, decrement
            // pin count and try again
            if (victim.dpid >= 0) {
                victim.pinCount.decrementAndGet();
                return null;
            }
            returnPage = victim;
            ((CachedPage) returnPage).dpid = dpid;
        } else {
            // Case 2a/b
            int pageHash = hash(victim.getDiskPageId());
            CacheBucket bucket = pageMap[pageHash];
            bucket.bucketLock.lock();
            try {
                // readjust the next pointers to remove this page from
                // the pagemap
                CachedPage curr = bucket.cachedPage;
                CachedPage prev = null;
                boolean found = false;
                //traverse the bucket's linked list to find the victim.
                while (curr != null) {
                    if (curr == victim) { // we found where the victim
                        // resides in the hash table
                        if (!victim.pinCount.compareAndSet(0, 1)) {
                            break;
                        }
                        if (DEBUG) {
                            assert curr != curr.next;
                        }
                        if (prev == null) {
                            // if this is the first page in the bucket
                            bucket.cachedPage = curr.next;
                            // if it isn't we need to make the previous
                            // node point to where it should
                        } else {
                            prev.next = curr.next;
                            if (DEBUG) {
                                assert prev.next != prev;
                            }
                        }
                        curr.next = null;
                        found = true;
                        break;
                    }
                    // go to the next entry
                    prev = curr;
                    curr = curr.next;
                }
                if (found) {
                    returnPage = victim;
                    ((CachedPage) returnPage).dpid = dpid;
                } //otherwise, someone took the same victim before we acquired the lock. try again!
            } finally {
                bucket.bucketLock.unlock();
            }
        }
        // if we found a page after all that, go ahead and finish
        if (returnPage != null) {
            ((CachedPage) returnPage).confiscated.set(true);
            if (DEBUG) {
                confiscateLock.lock();
                try {
                    confiscatedPages.add((CachedPage) returnPage);
                    confiscatedPagesOwner.put((CachedPage) returnPage, Thread.currentThread().getStackTrace());
                } finally {
                    confiscateLock.unlock();
                }
            }
            return returnPage;
        }
        return null;
    }

    private BufferedFileHandle getOrCreateFileHandle(int fileId) throws HyracksDataException {
        synchronized (fileInfoMap) {
            final FileReference fileRef = fileMapManager.lookupFileName(fileId);
            return fileInfoMap.computeIfAbsent(fileId, id -> BufferedFileHandle.create(fileRef, fileId, this, ioManager,
                    headerPageCache, pageReplacementStrategy));
        }
    }

    private BufferedFileHandle removeFileHandle(int fileId) {
        synchronized (fileInfoMap) {
            return fileInfoMap.remove(fileId);
        }
    }

    private BufferedFileHandle getFileHandle(CachedPage cPage) throws HyracksDataException {
        return getFileHandle(BufferedFileHandle.getFileId(cPage.dpid));
    }

    private BufferedFileHandle getFileHandle(int fileId) throws HyracksDataException {
        BufferedFileHandle fInfo;
        synchronized (fileInfoMap) {
            fInfo = fileInfoMap.get(fileId);
        }
        if (fInfo == null) {
            throw HyracksDataException.create(ErrorCode.FILE_DOES_NOT_EXIST, fileId);
        }
        return fInfo;
    }

    private ICachedPage getPageLoop(long dpid, int multiplier, boolean confiscate) throws HyracksDataException {
        final long startingPinCount = DEBUG ? masterPinCount.get() : -1;
        int cycleCount = 0;
        try {
            while (true) {
                cycleCount++;
                int startCleanedCount = cleanerThread.cleanedCount;
                ICachedPage page = confiscate ? confiscateInner(dpid, multiplier) : findPageInner(dpid);
                if (page != null) {
                    masterPinCount.incrementAndGet();
                    return page;
                }
                // no page available to confiscate. try kicking the cleaner thread.
                synchronized (cleanerThread.threadLock) {
                    try {
                        pageCleanerPolicy.notifyVictimNotFound(cleanerThread.threadLock);
                    } catch (InterruptedException e) {
                        // Re-interrupt the thread so this gets handled later
                        Thread.currentThread().interrupt();
                    }
                }
                // Heuristic optimization. Check whether the cleaner thread has
                // cleaned pages since we did our last pin attempt.
                if (cleanerThread.cleanedCount - startCleanedCount > MIN_CLEANED_COUNT_DIFF) {
                    // Don't go to sleep and wait for notification from the cleaner,
                    // just try to pin again immediately.
                    continue;
                }
                synchronized (cleanerThread.cleanNotification) {
                    try {
                        // it's OK to not loop on this wait, as we do not rely on any condition to be true on notify
                        // This seemingly pointless loop keeps SonarQube happy
                        do {
                            cleanerThread.cleanNotification.wait(PIN_MAX_WAIT_TIME);
                        } while (false);
                    } catch (InterruptedException e) {
                        // Re-interrupt the thread so this gets handled later
                        Thread.currentThread().interrupt();
                    }
                }
                if (cycleCount > MAX_PIN_ATTEMPT_CYCLES) {
                    cycleCount = 0; // suppress warning below
                    throw new HyracksDataException("Unable to find free page in buffer cache after "
                            + MAX_PIN_ATTEMPT_CYCLES + " cycles (buffer cache undersized?)"
                            + (DEBUG ? " ; " + (masterPinCount.get() - startingPinCount)
                                    + " successful pins since start of cycle" : ""));
                }
            }
        } finally {
            if (cycleCount > PIN_ATTEMPT_CYCLES_WARNING_THRESHOLD && LOGGER.isWarnEnabled()) {
                LOGGER.warn("Took " + cycleCount + " cycles to find free page in buffer cache.  (buffer cache "
                        + "undersized?)" + (DEBUG ? " ; " + (masterPinCount.get() - startingPinCount)
                                + " successful pins since start of cycle" : ""));
            }
        }
    }

    @Override
    public void returnPage(ICachedPage page) {
        returnPage(page, true);
    }

    @Override
    public void returnPage(ICachedPage page, boolean reinsert) {
        CachedPage cPage = (CachedPage) page;
        CacheBucket bucket;
        if (!page.confiscated()) {
            return;
        }
        if (reinsert) {
            int hash = hash(cPage.dpid);
            bucket = pageMap[hash];
            bucket.bucketLock.lock();
            if (DEBUG) {
                confiscateLock.lock();
            }
            try {
                cPage.reset(cPage.dpid);
                cPage.valid = true;
                cPage.next = bucket.cachedPage;
                bucket.cachedPage = cPage;
                cPage.pinCount.decrementAndGet();
                if (DEBUG) {
                    assert cPage.pinCount.get() == 0;
                    assert cPage.latch.getReadLockCount() == 0;
                    assert cPage.latch.getWriteHoldCount() == 0;
                    confiscatedPages.remove(cPage);
                    confiscatedPagesOwner.remove(cPage);
                }
            } finally {
                bucket.bucketLock.unlock();
                if (DEBUG) {
                    confiscateLock.unlock();
                }
            }
        } else {
            cPage.invalidate();
            cPage.pinCount.decrementAndGet();
            if (DEBUG) {
                assert cPage.pinCount.get() == 0;
                assert cPage.latch.getReadLockCount() == 0;
                assert cPage.latch.getWriteHoldCount() == 0;
                confiscateLock.lock();
                try {
                    confiscatedPages.remove(cPage);
                    confiscatedPagesOwner.remove(cPage);
                } finally {
                    confiscateLock.unlock();
                }
            }
        }
        pageReplacementStrategy.adviseWontNeed(cPage);
    }

    @Override
    public IFIFOPageWriter createFIFOWriter(IPageWriteCallback callback, IPageWriteFailureCallback failureCallback) {
        return new FIFOLocalWriter(this, callback, failureCallback);
    }

    @Override
    public boolean isReplicationEnabled() {
        if (ioReplicationManager != null) {
            return ioReplicationManager.isReplicationEnabled();
        }
        return false;
    }

    @Override
    public IIOReplicationManager getIOReplicationManager() {
        return ioReplicationManager;
    }

    @Override
    public void purgeHandle(int fileId) throws HyracksDataException {
        BufferedFileHandle fh = removeFileHandle(fileId);
        if (fh != null) {
            synchronized (fileInfoMap) {
                fileMapManager.unregisterFile(fileId);
                fh.purge();
            }
        }

    }

    @Override
    public void closeFileIfOpen(FileReference fileRef) {
        synchronized (fileInfoMap) {
            if (fileMapManager.isMapped(fileRef)) {
                int fileId;
                try {
                    fileId = fileMapManager.lookupFileId(fileRef);
                } catch (HyracksDataException e) {
                    throw new IllegalStateException(e);
                }
                BufferedFileHandle fInfo = fileInfoMap.get(fileId);
                if (fInfo != null && fInfo.getReferenceCount() > 0) {
                    fInfo.decReferenceCount();
                }
            }
        }
    }

    @Override
    public ICompressedPageWriter getCompressedPageWriter(int fileId) {
        final BufferedFileHandle fInfo;
        synchronized (fileInfoMap) {
            fInfo = fileInfoMap.get(fileId);
        }

        return fInfo.getCompressedPageWriter();
    }

}
