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
package edu.uci.ics.hyracks.storage.common.buffercache;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IFileHandle;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.lifecycle.ILifeCycleComponent;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;

public class BufferCache implements IBufferCacheInternal, ILifeCycleComponent {
    private static final Logger LOGGER = Logger.getLogger(BufferCache.class.getName());
    private static final int MAP_FACTOR = 2;

    private static final int MIN_CLEANED_COUNT_DIFF = 3;
    private static final int PIN_MAX_WAIT_TIME = 50;

    private final int maxOpenFiles;

    private final IIOManager ioManager;
    private final int pageSize;
    private final int numPages;
    private final CacheBucket[] pageMap;
    private final IPageReplacementStrategy pageReplacementStrategy;
    private final IPageCleanerPolicy pageCleanerPolicy;
    private final IFileMapManager fileMapManager;
    private final CleanerThread cleanerThread;
    private final Map<Integer, BufferedFileHandle> fileInfoMap;

    private CachedPage[] cachedPages;

    private boolean closed;

    public BufferCache(IIOManager ioManager, ICacheMemoryAllocator allocator,
            IPageReplacementStrategy pageReplacementStrategy, IPageCleanerPolicy pageCleanerPolicy,
            IFileMapManager fileMapManager, int pageSize, int numPages, int maxOpenFiles, ThreadFactory threadFactory) {
        this.ioManager = ioManager;
        this.pageSize = pageSize;
        this.numPages = numPages;
        this.maxOpenFiles = maxOpenFiles;
        pageReplacementStrategy.setBufferCache(this);
        pageMap = new CacheBucket[numPages * MAP_FACTOR];
        for (int i = 0; i < pageMap.length; ++i) {
            pageMap[i] = new CacheBucket();
        }
        ByteBuffer[] buffers = allocator.allocate(pageSize, numPages);
        cachedPages = new CachedPage[buffers.length];
        for (int i = 0; i < buffers.length; ++i) {
            cachedPages[i] = new CachedPage(i, buffers[i], pageReplacementStrategy);
        }
        this.pageReplacementStrategy = pageReplacementStrategy;
        this.pageCleanerPolicy = pageCleanerPolicy;
        this.fileMapManager = fileMapManager;
        Executor executor = Executors.newCachedThreadPool(threadFactory);
        fileInfoMap = new HashMap<Integer, BufferedFileHandle>();
        cleanerThread = new CleanerThread();
        executor.execute(cleanerThread);
        closed = false;
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    @Override
    public int getNumPages() {
        return numPages;
    }

    private void pinSanityCheck(long dpid) throws HyracksDataException {
        if (closed) {
            throw new HyracksDataException("pin called on a closed cache");
        }

        // check whether file has been created and opened
        int fileId = BufferedFileHandle.getFileId(dpid);
        BufferedFileHandle fInfo = null;
        synchronized (fileInfoMap) {
            fInfo = fileInfoMap.get(fileId);
        }
        if (fInfo == null) {
            throw new HyracksDataException("pin called on a fileId " + fileId + " that has not been created.");
        } else if (fInfo.getReferenceCount() <= 0) {
            throw new HyracksDataException("pin called on a fileId " + fileId + " that has not been opened.");
        }
    }

    @Override
    public ICachedPage tryPin(long dpid) throws HyracksDataException {
        // Calling the pinSanityCheck should be used only for debugging, since the synchronized block over the fileInfoMap is a hot spot.
        //pinSanityCheck(dpid);
        CachedPage cPage = null;
        int hash = hash(dpid);
        CacheBucket bucket = pageMap[hash];
        bucket.bucketLock.lock();
        try {
            cPage = bucket.cachedPage;
            while (cPage != null) {
                if (cPage.dpid == dpid) {
                    cPage.pinCount.incrementAndGet();
                    pageReplacementStrategy.notifyCachePageAccess(cPage);
                    return cPage;
                }
                cPage = cPage.next;
            }
        } finally {
            bucket.bucketLock.unlock();
        }
        return cPage;
    }

    @Override
    public ICachedPage pin(long dpid, boolean newPage) throws HyracksDataException {
        // Calling the pinSanityCheck should be used only for debugging, since the synchronized block over the fileInfoMap is a hot spot.
        //pinSanityCheck(dpid);
        CachedPage cPage = findPage(dpid, newPage);
        if (!newPage) {
            // Resolve race of multiple threads trying to read the page from
            // disk.
            synchronized (cPage) {
                if (!cPage.valid) {
                    read(cPage);
                    cPage.valid = true;
                }
            }
        } else {
            cPage.valid = true;
        }
        pageReplacementStrategy.notifyCachePageAccess(cPage);
        return cPage;
    }

    private CachedPage findPage(long dpid, boolean newPage) throws HyracksDataException {
        while (true) {
            int startCleanedCount = cleanerThread.cleanedCount;

            CachedPage cPage = null;
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
                    if (cPage.dpid == dpid) {
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
            if (victim != null) {
                /*
                 * We have a victim with the following invariants. 1. The dpid
                 * on the CachedPage may or may not be valid. 2. We have a pin
                 * on the CachedPage. We have to deal with three cases here.
                 * Case 1: The dpid on the CachedPage is invalid (-1). This
                 * indicates that this buffer has never been used. So we are the
                 * only ones holding it. Get a lock on the required dpid's hash
                 * bucket, check if someone inserted the page we want into the
                 * table. If so, decrement the pincount on the victim and return
                 * the winner page in the table. If such a winner does not
                 * exist, insert the victim and return it. Case 2: The dpid on
                 * the CachedPage is valid. Case 2a: The current dpid and
                 * required dpid hash to the same bucket. Get the bucket lock,
                 * check that the victim is still at pinCount == 1 If so check
                 * if there is a winning CachedPage with the required dpid. If
                 * so, decrement the pinCount on the victim and return the
                 * winner. If not, update the contents of the CachedPage to hold
                 * the required dpid and return it. If the picCount on the
                 * victim was != 1 or CachedPage was dirty someone used the
                 * victim for its old contents -- Decrement the pinCount and
                 * retry. Case 2b: The current dpid and required dpid hash to
                 * different buckets. Get the two bucket locks in the order of
                 * the bucket indexes (Ordering prevents deadlocks). Check for
                 * the existence of a winner in the new bucket and for potential
                 * use of the victim (pinCount != 1). If everything looks good,
                 * remove the CachedPage from the old bucket, and add it to the
                 * new bucket and update its header with the new dpid.
                 */
                if (victim.dpid < 0) {
                    /*
                     * Case 1.
                     */
                    bucket.bucketLock.lock();
                    try {
                        cPage = bucket.cachedPage;
                        while (cPage != null) {
                            if (cPage.dpid == dpid) {
                                cPage.pinCount.incrementAndGet();
                                victim.pinCount.decrementAndGet();
                                return cPage;
                            }
                            cPage = cPage.next;
                        }
                        victim.reset(dpid);
                        victim.next = bucket.cachedPage;
                        bucket.cachedPage = victim;
                    } finally {
                        bucket.bucketLock.unlock();
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
                        if (victim.pinCount.get() != 1) {
                            victim.pinCount.decrementAndGet();
                            continue;
                        }
                        cPage = bucket.cachedPage;
                        while (cPage != null) {
                            if (cPage.dpid == dpid) {
                                cPage.pinCount.incrementAndGet();
                                victim.pinCount.decrementAndGet();
                                return cPage;
                            }
                            cPage = cPage.next;
                        }
                        victim.reset(dpid);
                    } finally {
                        bucket.bucketLock.unlock();
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
                        if (victim.pinCount.get() != 1) {
                            victim.pinCount.decrementAndGet();
                            continue;
                        }
                        cPage = bucket.cachedPage;
                        while (cPage != null) {
                            if (cPage.dpid == dpid) {
                                cPage.pinCount.incrementAndGet();
                                victim.pinCount.decrementAndGet();
                                return cPage;
                            }
                            cPage = cPage.next;
                        }
                        if (victimBucket.cachedPage == victim) {
                            victimBucket.cachedPage = victim.next;
                        } else {
                            CachedPage victimPrev = victimBucket.cachedPage;
                            while (victimPrev != null && victimPrev.next != victim) {
                                victimPrev = victimPrev.next;
                            }
                            assert victimPrev != null;
                            victimPrev.next = victim.next;
                        }
                        victim.reset(dpid);
                        victim.next = bucket.cachedPage;
                        bucket.cachedPage = victim;
                    } finally {
                        victimBucket.bucketLock.unlock();
                        bucket.bucketLock.unlock();
                    }
                    return victim;
                }
            }
            synchronized (cleanerThread) {
                pageCleanerPolicy.notifyVictimNotFound(cleanerThread);
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
                    cleanerThread.cleanNotification.wait(PIN_MAX_WAIT_TIME);
                } catch (InterruptedException e) {
                    // Do nothing
                }
            }
        }
    }

    private String dumpState() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("Buffer cache state\n");
        buffer.append("Page Size: ").append(pageSize).append('\n');
        buffer.append("Number of physical pages: ").append(numPages).append('\n');
        buffer.append("Hash table size: ").append(pageMap.length).append('\n');
        buffer.append("Page Map:\n");
        int nCachedPages = 0;
        for (int i = 0; i < pageMap.length; ++i) {
            CacheBucket cb = pageMap[i];
            cb.bucketLock.lock();
            try {
                CachedPage cp = cb.cachedPage;
                if (cp != null) {
                    buffer.append("   ").append(i).append('\n');
                    while (cp != null) {
                        buffer.append("      ").append(cp.cpid).append(" -> [")
                                .append(BufferedFileHandle.getFileId(cp.dpid)).append(':')
                                .append(BufferedFileHandle.getPageId(cp.dpid)).append(", ").append(cp.pinCount.get())
                                .append(", ").append(cp.valid ? "valid" : "invalid").append(", ")
                                .append(cp.dirty.get() ? "dirty" : "clean").append("]\n");
                        cp = cp.next;
                        ++nCachedPages;
                    }
                }
            } finally {
                cb.bucketLock.unlock();
            }
        }
        buffer.append("Number of cached pages: ").append(nCachedPages).append('\n');
        return buffer.toString();
    }

    private void read(CachedPage cPage) throws HyracksDataException {
        BufferedFileHandle fInfo = getFileInfo(cPage);
        cPage.buffer.clear();
        ioManager.syncRead(fInfo.getFileHandle(), (long) BufferedFileHandle.getPageId(cPage.dpid) * pageSize,
                cPage.buffer);
    }

    private BufferedFileHandle getFileInfo(CachedPage cPage) throws HyracksDataException {
        synchronized (fileInfoMap) {
            BufferedFileHandle fInfo = fileInfoMap.get(BufferedFileHandle.getFileId(cPage.dpid));
            if (fInfo == null) {
                throw new HyracksDataException("No such file mapped");
            }
            return fInfo;
        }
    }

    private void write(CachedPage cPage) throws HyracksDataException {
        BufferedFileHandle fInfo = getFileInfo(cPage);
        if (fInfo.fileHasBeenDeleted()) {
            return;
        }
        cPage.buffer.position(0);
        cPage.buffer.limit(pageSize);
        ioManager.syncWrite(fInfo.getFileHandle(), (long) BufferedFileHandle.getPageId(cPage.dpid) * pageSize,
                cPage.buffer);
    }

    @Override
    public void unpin(ICachedPage page) throws HyracksDataException {
        if (closed) {
            throw new HyracksDataException("unpin called on a closed cache");
        }
        ((CachedPage) page).pinCount.decrementAndGet();
    }

    private int hash(long dpid) {
        return (int) (dpid % pageMap.length);
    }

    private static class CacheBucket {
        private final Lock bucketLock;
        private CachedPage cachedPage;

        public CacheBucket() {
            bucketLock = new ReentrantLock();
        }
    }

    private class CachedPage implements ICachedPageInternal {
        private final int cpid;
        private final ByteBuffer buffer;
        private final AtomicInteger pinCount;
        private final AtomicBoolean dirty;
        private final ReadWriteLock latch;
        private final Object replacementStrategyObject;
        volatile long dpid;
        CachedPage next;
        volatile boolean valid;

        public CachedPage(int cpid, ByteBuffer buffer, IPageReplacementStrategy pageReplacementStrategy) {
            this.cpid = cpid;
            this.buffer = buffer;
            pinCount = new AtomicInteger();
            dirty = new AtomicBoolean();
            latch = new ReentrantReadWriteLock(true);
            replacementStrategyObject = pageReplacementStrategy.createPerPageStrategyObject(cpid);
            dpid = -1;
            valid = false;
        }

        public void reset(long dpid) {
            this.dpid = dpid;
            dirty.set(false);
            valid = false;
            pageReplacementStrategy.notifyCachePageReset(this);
        }

        public void invalidate() {
            reset(-1);
        }

        @Override
        public ByteBuffer getBuffer() {
            return buffer;
        }

        @Override
        public Object getReplacementStrategyObject() {
            return replacementStrategyObject;
        }

        @Override
        public boolean pinIfGoodVictim() {
            return pinCount.compareAndSet(0, 1);
        }

        @Override
        public int getCachedPageId() {
            return cpid;
        }

        @Override
        public void acquireReadLatch() {
            latch.readLock().lock();
        }

        private void acquireWriteLatch(boolean markDirty) {
            latch.writeLock().lock();
            if (markDirty) {
                if (dirty.compareAndSet(false, true)) {
                    pinCount.incrementAndGet();
                }
            }
        }

        @Override
        public void acquireWriteLatch() {
            acquireWriteLatch(true);
        }

        @Override
        public void releaseReadLatch() {
            latch.readLock().unlock();
        }

        @Override
        public void releaseWriteLatch() {
            latch.writeLock().unlock();
        }
    }

    @Override
    public ICachedPageInternal getPage(int cpid) {
        return cachedPages[cpid];
    }

    private class CleanerThread extends Thread {
        private boolean shutdownStart = false;
        private boolean shutdownComplete = false;
        private final Object cleanNotification = new Object();
        // Simply keeps incrementing this counter when a page is cleaned.
        // Used to implement wait-for-cleanerthread heuristic optimizations.
        // A waiter can detect whether pages have been cleaned.
        // No need to make this var volatile or synchronize it's access in any
        // way because it is used for heuristics.
        private int cleanedCount = 0;

        public CleanerThread() {
            setPriority(MAX_PRIORITY);
            setDaemon(true);
        }

        public void cleanPage(CachedPage cPage, boolean force) {
            if (cPage.dirty.get()) {
                boolean proceed = false;
                if (force) {
                    cPage.latch.writeLock().lock();
                    proceed = true;
                } else {
                    proceed = cPage.latch.readLock().tryLock();
                }
                if (proceed) {
                    try {
                        // Make sure page is still dirty.
                        if (!cPage.dirty.get()) {
                            return;
                        }
                        boolean cleaned = true;
                        try {
                            write(cPage);
                        } catch (HyracksDataException e) {
                            cleaned = false;
                        }
                        if (cleaned) {
                            cPage.dirty.set(false);
                            cPage.pinCount.decrementAndGet();
                            cleanedCount++;
                            synchronized (cleanNotification) {
                                cleanNotification.notifyAll();
                            }
                        }
                    } finally {
                        if (force) {
                            cPage.latch.writeLock().unlock();
                        } else {
                            cPage.latch.readLock().unlock();
                        }
                    }
                } else if (shutdownStart) {
                    throw new IllegalStateException("Cache closed, but unable to acquire read lock on dirty page: "
                            + cPage.dpid);
                }
            }
        }

        @Override
        public synchronized void run() {
            try {
                while (true) {
                    pageCleanerPolicy.notifyCleanCycleStart(this);
                    for (int i = 0; i < numPages; ++i) {
                        CachedPage cPage = cachedPages[i];
                        cleanPage(cPage, false);
                    }
                    if (shutdownStart) {
                        break;
                    }
                    pageCleanerPolicy.notifyCleanCycleFinish(this);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                shutdownComplete = true;
                notifyAll();
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        synchronized (cleanerThread) {
            cleanerThread.shutdownStart = true;
            cleanerThread.notifyAll();
            while (!cleanerThread.shutdownComplete) {
                try {
                    cleanerThread.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        synchronized (fileInfoMap) {
            try {
                for (Map.Entry<Integer, BufferedFileHandle> entry : fileInfoMap.entrySet()) {
                    boolean fileHasBeenDeleted = entry.getValue().fileHasBeenDeleted();
                    sweepAndFlush(entry.getKey(), !fileHasBeenDeleted);
                    if (!fileHasBeenDeleted) {
                        ioManager.close(entry.getValue().getFileHandle());
                    }
                }
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }
            fileInfoMap.clear();
        }
    }

    @Override
    public void createFile(FileReference fileRef) throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Creating file: " + fileRef + " in cache: " + this);
        }
        synchronized (fileInfoMap) {
            fileMapManager.registerFile(fileRef);
        }
    }

    @Override
    public void openFile(int fileId) throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Opening file: " + fileId + " in cache: " + this);
        }
        synchronized (fileInfoMap) {
            BufferedFileHandle fInfo;
            fInfo = fileInfoMap.get(fileId);
            if (fInfo == null) {

                // map is full, make room by cleaning up unreferenced files
                boolean unreferencedFileFound = true;
                while (fileInfoMap.size() >= maxOpenFiles && unreferencedFileFound) {
                    unreferencedFileFound = false;
                    for (Map.Entry<Integer, BufferedFileHandle> entry : fileInfoMap.entrySet()) {
                        if (entry.getValue().getReferenceCount() <= 0) {
                            int entryFileId = entry.getKey();
                            boolean fileHasBeenDeleted = entry.getValue().fileHasBeenDeleted();
                            sweepAndFlush(entryFileId, !fileHasBeenDeleted);
                            if (!fileHasBeenDeleted) {
                                ioManager.close(entry.getValue().getFileHandle());
                            }
                            fileInfoMap.remove(entryFileId);
                            unreferencedFileFound = true;
                            // for-each iterator is invalid because we changed
                            // fileInfoMap
                            break;
                        }
                    }
                }

                if (fileInfoMap.size() >= maxOpenFiles) {
                    throw new HyracksDataException("Could not open fileId " + fileId + ". Max number of files "
                            + maxOpenFiles + " already opened and referenced.");
                }

                // create, open, and map new file reference
                FileReference fileRef = fileMapManager.lookupFileName(fileId);
                IFileHandle fh = ioManager.open(fileRef, IIOManager.FileReadWriteMode.READ_WRITE,
                        IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
                fInfo = new BufferedFileHandle(fileId, fh);
                fileInfoMap.put(fileId, fInfo);
            }
            fInfo.incReferenceCount();
        }
    }

    private void sweepAndFlush(int fileId, boolean flushDirtyPages) throws HyracksDataException {
        for (int i = 0; i < pageMap.length; ++i) {
            CacheBucket bucket = pageMap[i];
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
                if (bucket.cachedPage != null) {
                    if (invalidateIfFileIdMatch(fileId, bucket.cachedPage, flushDirtyPages)) {
                        CachedPage cPage = bucket.cachedPage;
                        bucket.cachedPage = bucket.cachedPage.next;
                        cPage.next = null;
                    }
                }
            } finally {
                bucket.bucketLock.unlock();
            }
        }
    }

    private boolean invalidateIfFileIdMatch(int fileId, CachedPage cPage, boolean flushDirtyPages)
            throws HyracksDataException {
        if (BufferedFileHandle.getFileId(cPage.dpid) == fileId) {
            int pinCount = -1;
            if (cPage.dirty.get()) {
                if (flushDirtyPages) {
                    write(cPage);
                }
                cPage.dirty.set(false);
                pinCount = cPage.pinCount.decrementAndGet();
            } else {
                pinCount = cPage.pinCount.get();
            }
            if (pinCount != 0) {
                throw new IllegalStateException("Page is pinned and file is being closed. Pincount is: " + pinCount);
            }
            cPage.invalidate();
            return true;
        }
        return false;
    }

    @Override
    public void closeFile(int fileId) throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Closing file: " + fileId + " in cache: " + this);
        }
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine(dumpState());
        }

        synchronized (fileInfoMap) {
            BufferedFileHandle fInfo = fileInfoMap.get(fileId);
            if (fInfo == null) {
                throw new HyracksDataException("Closing unopened file");
            }
            if (fInfo.decReferenceCount() < 0) {
                throw new HyracksDataException("Closed fileId: " + fileId + " more times than it was opened.");
            }
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Closed file: " + fileId + " in cache: " + this);
        }
    }

    @Override
    public void flushDirtyPage(ICachedPage page) throws HyracksDataException {
        // Assumes the caller has pinned the page.
        cleanerThread.cleanPage((CachedPage) page, true);
    }

    @Override
    public void force(int fileId, boolean metadata) throws HyracksDataException {
        BufferedFileHandle fInfo = null;
        synchronized (fileInfoMap) {
            fInfo = fileInfoMap.get(fileId);
        }
        ioManager.sync(fInfo.getFileHandle(), metadata);
    }

    @Override
    public synchronized void deleteFile(int fileId, boolean flushDirtyPages) throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Deleting file: " + fileId + " in cache: " + this);
        }
        if (flushDirtyPages) {
            synchronized (fileInfoMap) {
                sweepAndFlush(fileId, flushDirtyPages);
            }
        }
        synchronized (fileInfoMap) {
            BufferedFileHandle fInfo = null;
            try {
                fInfo = fileInfoMap.get(fileId);
                if (fInfo != null && fInfo.getReferenceCount() > 0) {
                    throw new HyracksDataException("Deleting open file");
                }
            } finally {
                fileMapManager.unregisterFile(fileId);
                if (fInfo != null) {
                    // Mark the fInfo as deleted,
                    // such that when its pages are reclaimed in openFile(),
                    // the pages are not flushed to disk but only invalidated.
                    if (!fInfo.fileHasBeenDeleted()) {
                        ioManager.close(fInfo.getFileHandle());
                        fInfo.markAsDeleted();
                    }
                }
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
            os.write(dumpState().getBytes());
        }
        close();
    }
}