/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileHandle;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;

public class BufferCache implements IBufferCacheInternal {
    private static final Logger LOGGER = Logger.getLogger(BufferCache.class.getName());
    private static final int MAP_FACTOR = 2;

    private static final int MAX_VICTIMIZATION_TRY_COUNT = 3;

    private final int maxOpenFiles;
    
    private final IIOManager ioManager;
    private final int pageSize;
    private final int numPages;
    private final CachedPage[] cachedPages;
    private final CacheBucket[] pageMap;
    private final IPageReplacementStrategy pageReplacementStrategy;
    private final IFileMapManager fileMapManager;
    private final CleanerThread cleanerThread;
    private final Map<Integer, BufferedFileHandle> fileInfoMap;

    private boolean closed;

    public BufferCache(IIOManager ioManager, ICacheMemoryAllocator allocator,
            IPageReplacementStrategy pageReplacementStrategy, IFileMapManager fileMapManager, int pageSize, int numPages, int maxOpenFiles) {
        this.ioManager = ioManager;
        this.pageSize = pageSize;
        this.numPages = numPages;
        this.maxOpenFiles = maxOpenFiles;
        pageReplacementStrategy.setBufferCache(this);
        ByteBuffer[] buffers = allocator.allocate(pageSize, numPages);
        cachedPages = new CachedPage[buffers.length];
        for (int i = 0; i < buffers.length; ++i) {
            cachedPages[i] = new CachedPage(i, buffers[i], pageReplacementStrategy);
        }
        pageMap = new CacheBucket[numPages * MAP_FACTOR];
        for (int i = 0; i < pageMap.length; ++i) {
            pageMap[i] = new CacheBucket();
        }
        this.pageReplacementStrategy = pageReplacementStrategy;
        this.fileMapManager = fileMapManager;
        fileInfoMap = new HashMap<Integer, BufferedFileHandle>();
        cleanerThread = new CleanerThread();
        cleanerThread.start();
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
        BufferedFileHandle fInfo = fileInfoMap.get(fileId);
        if(fInfo == null) {
            throw new HyracksDataException("pin called on a fileId " + fileId + " that has not been created.");
        } else if(fInfo.getReferenceCount() <= 0) {
            throw new HyracksDataException("pin called on a fileId " + fileId + " that has not been opened.");
        }
    }
    
    @Override 
    public ICachedPage tryPin(long dpid) throws HyracksDataException {        
        pinSanityCheck(dpid);
        
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
        pinSanityCheck(dpid);
        
        CachedPage cPage = findPage(dpid, newPage);
        if (!newPage) {
            if (!cPage.valid) {
                /*
                 * We got a buffer and we have pinned it. But its invalid. If its a new page, we just mark it as valid
                 * and return. Or else, while we hold the page lock, we get a write latch on the data and start a read.
                 */
                cPage.acquireWriteLatch(false);
                try {
                    if (!cPage.valid) {
                        read(cPage);
                    }
                    cPage.valid = true;
                } finally {
                    cPage.releaseWriteLatch();
                }
            }
        } else {
            cPage.valid = true;
        }
        pageReplacementStrategy.notifyCachePageAccess(cPage);
        return cPage;
    }

    private CachedPage findPage(long dpid, boolean newPage) {
        int victimizationTryCount = 0;
        int localCleanCount = -1;
        synchronized (cleanerThread.cleanNotification) {
            localCleanCount = cleanerThread.cleanCount;
        }
        while (true) {
            CachedPage cPage = null;
            /*
             * Hash dpid to get a bucket and then check if the page exists in the bucket.
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
             * If we got here, the page was not in the hash table. Now we ask the page replacement strategy to find us a victim.
             */
            CachedPage victim = (CachedPage) pageReplacementStrategy.findVictim();
            if (victim != null) {
                /*
                 * We have a victim with the following invariants.
                 * 1. The dpid on the CachedPage may or may not be valid.
                 * 2. We have a pin on the CachedPage. We have to deal with three cases here.
                 *  Case 1: The dpid on the CachedPage is invalid (-1). This indicates that this buffer has never been used.
                 *  So we are the only ones holding it. Get a lock on the required dpid's hash bucket, check if someone inserted
                 *  the page we want into the table. If so, decrement the pincount on the victim and return the winner page in the
                 *  table. If such a winner does not exist, insert the victim and return it.
                 *  Case 2: The dpid on the CachedPage is valid.
                 *      Case 2a: The current dpid and required dpid hash to the same bucket.
                 *      Get the bucket lock, check that the victim is still at pinCount == 1 If so check if there is a winning
                 *      CachedPage with the required dpid. If so, decrement the pinCount on the victim and return the winner.
                 *      If not, update the contents of the CachedPage to hold the required dpid and return it. If the picCount
                 *      on the victim was != 1 or CachedPage was dirty someone used the victim for its old contents -- Decrement
                 *      the pinCount and retry.
                 *  Case 2b: The current dpid and required dpid hash to different buckets. Get the two bucket locks in the order
                 *  of the bucket indexes (Ordering prevents deadlocks). Check for the existence of a winner in the new bucket
                 *  and for potential use of the victim (pinCount != 1). If everything looks good, remove the CachedPage from
                 *  the old bucket, and add it to the new bucket and update its header with the new dpid.
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
            /*
             * Victimization failed -- all pages pinned? wait a bit, increment victimizationTryCount and loop around. Give up after MAX_VICTIMIZATION_TRY_COUNT trys.
             */
            if (++victimizationTryCount >= MAX_VICTIMIZATION_TRY_COUNT) {
                return null;
            }
            synchronized (cleanerThread) {
                cleanerThread.notifyAll();
            }
            synchronized (cleanerThread.cleanNotification) {
                if (cleanerThread.cleanCount == localCleanCount) {
                    try {
                        cleanerThread.cleanNotification.wait(1000);
                    } catch (InterruptedException e) {
                        // Do nothing
                    }
                }
            }
        }
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
        private int cleanCount = 0;

        public CleanerThread() {
            setPriority(MAX_PRIORITY);
        }

        @Override
        public synchronized void run() {
            try {
                while (true) {
                    for (int i = 0; i < numPages; ++i) {
                        CachedPage cPage = cachedPages[i];
                        if (cPage.dirty.get()) {
                            if (cPage.latch.readLock().tryLock()) {
                                try {
                                    boolean cleaned = true;
                                    try {
                                        write(cPage);
                                    } catch (HyracksDataException e) {
                                        cleaned = false;
                                    }
                                    if (cleaned) {
                                        cPage.dirty.set(false);
                                        cPage.pinCount.decrementAndGet();
                                        synchronized (cleanNotification) {
                                            ++cleanCount;
                                            cleanNotification.notifyAll();
                                        }
                                    }
                                } finally {
                                    cPage.latch.readLock().unlock();
                                }
                            } else if (shutdownStart) {
                                throw new IllegalStateException(
                                        "Cache closed, but unable to acquire read lock on dirty page: " + cPage.dpid);
                            }
                        }
                    }
                    if (shutdownStart) {
                        break;
                    }
                    try {
                        wait(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
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
                for(Map.Entry<Integer, BufferedFileHandle> entry : fileInfoMap.entrySet()) {
                    sweepAndFlush(entry.getKey());
                    ioManager.close(entry.getValue().getFileHandle());
                }
            } catch(HyracksDataException e) {
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
                
                // map is full, make room by removing cleaning up unreferenced files
                boolean unreferencedFileFound = true;
                while(fileInfoMap.size() >= maxOpenFiles && unreferencedFileFound) {                
                    unreferencedFileFound = false;                    
                    for(Map.Entry<Integer, BufferedFileHandle> entry : fileInfoMap.entrySet()) {
                        if(entry.getValue().getReferenceCount() <= 0) {
                            int entryFileId = entry.getKey();
                            sweepAndFlush(entryFileId);
                            fileInfoMap.remove(entryFileId);
                            ioManager.close(entry.getValue().getFileHandle());
                            unreferencedFileFound = true;
                            // for-each iterator is invalid because we changed fileInfoMap
                            break;
                        }
                    }
                }
                
                if(fileInfoMap.size() >= maxOpenFiles) {
                    throw new HyracksDataException("Could not open fileId " + fileId + ". Max number of files " + maxOpenFiles + " already opened and referenced.");
                }
                
                // create, open, and map new file reference
                FileReference fileRef = fileMapManager.lookupFileName(fileId);
                FileHandle fh = ioManager.open(fileRef, IIOManager.FileReadWriteMode.READ_WRITE,
                        IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
                fInfo = new BufferedFileHandle(fileId, fh);
                fileInfoMap.put(fileId, fInfo);
            }
            fInfo.incReferenceCount();
        }
    }
        
    private void sweepAndFlush(int fileId) throws HyracksDataException {
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
                    if (invalidateIfFileIdMatch(fileId, cPage)) {
                        prev.next = cPage.next;
                        cPage.next = null;
                    } else {
                        prev = cPage;
                    }
                }
                // Take care of the head of the chain.
                if (bucket.cachedPage != null) {
                    if (invalidateIfFileIdMatch(fileId, bucket.cachedPage)) {
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

    private boolean invalidateIfFileIdMatch(int fileId, CachedPage cPage) throws HyracksDataException {
        if (BufferedFileHandle.getFileId(cPage.dpid) == fileId) {
            if (cPage.dirty.get()) {
                write(cPage);
                cPage.dirty.set(false);
                cPage.pinCount.decrementAndGet();
            }
            if (cPage.pinCount.get() != 0) {
                throw new IllegalStateException("Page is pinned and file is being closed");
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
        synchronized (fileInfoMap) {
            BufferedFileHandle fInfo = fileInfoMap.get(fileId);
            if (fInfo == null) {
                throw new HyracksDataException("Closing unopened file");
            }
            if (fInfo.decReferenceCount() < 0) {
                throw new HyracksDataException("Closed fileId: " + fileId + " more times than it was opened.");                                
            }
        }
    }

    @Override
    public synchronized void deleteFile(int fileId) throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Deleting file: " + fileId + " in cache: " + this);
        }
        synchronized (fileInfoMap) {
            BufferedFileHandle fInfo = fileInfoMap.get(fileId);
            if (fInfo != null) {
                throw new HyracksDataException("Deleting open file");
            }
            fileMapManager.unregisterFile(fileId);
        }
    }
}