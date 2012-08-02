/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.common.freepage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCacheInternal;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPageInternal;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class InMemoryBufferCache implements IBufferCacheInternal {
    protected final ICacheMemoryAllocator allocator;
    protected final IFileMapManager fileMapManager;
    protected final int pageSize;
    protected final int numPages;
    protected final List<CachedPage> overflowPages = new ArrayList<CachedPage>();
    protected CachedPage[] pages;

    public InMemoryBufferCache(ICacheMemoryAllocator allocator, int pageSize, int numPages,
            IFileMapManager fileMapManager) {
        this.allocator = allocator;
        this.fileMapManager = fileMapManager;
        this.pageSize = pageSize;
        this.numPages = numPages;
    }

    public void open() {
        pages = new CachedPage[numPages];
        ByteBuffer[] buffers = allocator.allocate(pageSize, numPages);
        for (int i = 0; i < buffers.length; ++i) {
            pages[i] = new CachedPage(i, buffers[i]);
        }
    }

    @Override
    public ICachedPage pin(long dpid, boolean newPage) {
        int pageId = BufferedFileHandle.getPageId(dpid);
        if (pageId < pages.length) {
            // Common case: Return regular page.
            return pages[pageId];
        } else {
            // Rare case: Return overflow page, possibly expanding overflow array.
            synchronized (overflowPages) {
                int numNewPages = pageId - pages.length - overflowPages.size() + 1;
                if (numNewPages > 0) {
                    ByteBuffer[] buffers = allocator.allocate(pageSize, numNewPages);
                    for (int i = 0; i < numNewPages; i++) {
                        CachedPage overflowPage = new CachedPage(pages.length + overflowPages.size(), buffers[i]);
                        overflowPages.add(overflowPage);
                    }
                }
                return overflowPages.get(pageId - pages.length);
            }
        }
    }

    @Override
    public ICachedPage tryPin(long dpid) throws HyracksDataException {
        return pin(dpid, false);
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    @Override
    public int getNumPages() {
        return pages.length;
    }

    @Override
    public ICachedPageInternal getPage(int cpid) {
        return pages[cpid];
    }

    public int getNumOverflowPages() {
        return overflowPages.size();
    }

    @Override
    public void createFile(FileReference fileRef) throws HyracksDataException {
        synchronized (fileMapManager) {
            fileMapManager.registerFile(fileRef);
        }
    }

    @Override
    public void openFile(int fileId) throws HyracksDataException {
        // Do nothing.
    }

    @Override
    public void closeFile(int fileId) throws HyracksDataException {
        // Do nothing.
    }

    @Override
    public void deleteFile(int fileId, boolean flushDirtyPages) throws HyracksDataException {
        synchronized (fileMapManager) {
            fileMapManager.unregisterFile(fileId);
        }
    }

    @Override
    public void unpin(ICachedPage page) throws HyracksDataException {
        // Do Nothing.
    }

    @Override
    public void close() {
        for (int i = 0; i < numPages; ++i) {
            pages[i] = null;
        }
        overflowPages.clear();
    }

    public class CachedPage implements ICachedPageInternal {
        private final int cpid;
        private final ByteBuffer buffer;
        private final ReadWriteLock latch;

        public CachedPage(int cpid, ByteBuffer buffer) {
            this.cpid = cpid;
            this.buffer = buffer;
            latch = new ReentrantReadWriteLock(true);
        }

        @Override
        public ByteBuffer getBuffer() {
            return buffer;
        }

        @Override
        public Object getReplacementStrategyObject() {
            // Do nothing.
            return null;
        }

        @Override
        public boolean pinIfGoodVictim() {
            // Do nothing.
            return false;
        }

        @Override
        public int getCachedPageId() {
            return cpid;
        }

        @Override
        public void acquireReadLatch() {
            latch.readLock().lock();
        }

        @Override
        public void acquireWriteLatch() {
            latch.writeLock().lock();
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
    public void force(int fileId, boolean metadata) throws HyracksDataException {
    }

    @Override
    public void flushDirtyPage(ICachedPage page) throws HyracksDataException {
    }

    public IFileMapProvider getFileMapProvider() {
        return fileMapManager;
    }
}
