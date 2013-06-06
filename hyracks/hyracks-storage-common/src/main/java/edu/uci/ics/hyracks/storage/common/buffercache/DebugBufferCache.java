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

import java.util.concurrent.atomic.AtomicLong;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;

/**
 * Implementation of an IBufferCache that counts the number of pins/unpins,
 * latches/unlatches, and file create/delete/open/close called on it. It
 * delegates the actual functionality to another IBufferCache set in the c'tor.
 * The counters are updated in a thread-safe fashion using AtomicLong.
 */
public class DebugBufferCache implements IBufferCache {

    // Actual BufferCache functionality is delegated to this bufferCache.
    private final IBufferCache bufferCache;
    private AtomicLong pinCount = new AtomicLong();
    private AtomicLong unpinCount = new AtomicLong();
    private AtomicLong readLatchCount = new AtomicLong();
    private AtomicLong readUnlatchCount = new AtomicLong();
    private AtomicLong writeLatchCount = new AtomicLong();
    private AtomicLong writeUnlatchCount = new AtomicLong();
    private AtomicLong createFileCount = new AtomicLong();
    private AtomicLong deleteFileCount = new AtomicLong();
    private AtomicLong openFileCount = new AtomicLong();
    private AtomicLong closeFileCount = new AtomicLong();

    public DebugBufferCache(IBufferCache bufferCache) {
        this.bufferCache = bufferCache;
        resetCounters();
    }

    @Override
    public void createFile(FileReference fileRef) throws HyracksDataException {
        bufferCache.createFile(fileRef);
        createFileCount.addAndGet(1);
    }

    @Override
    public void openFile(int fileId) throws HyracksDataException {
        bufferCache.openFile(fileId);
        openFileCount.addAndGet(1);
    }

    @Override
    public void closeFile(int fileId) throws HyracksDataException {
        bufferCache.closeFile(fileId);
        closeFileCount.addAndGet(1);
    }

    @Override
    public void deleteFile(int fileId, boolean flushDirtyPages) throws HyracksDataException {
        bufferCache.deleteFile(fileId, flushDirtyPages);
        deleteFileCount.addAndGet(1);
    }

    @Override
    public ICachedPage tryPin(long dpid) throws HyracksDataException {
        return bufferCache.tryPin(dpid);
    }

    @Override
    public ICachedPage pin(long dpid, boolean newPage) throws HyracksDataException {
        ICachedPage page = bufferCache.pin(dpid, newPage);
        pinCount.addAndGet(1);
        return page;
    }

    @Override
    public void unpin(ICachedPage page) throws HyracksDataException {
        bufferCache.unpin(page);
        unpinCount.addAndGet(1);
    }

    @Override
    public int getPageSize() {
        return bufferCache.getPageSize();
    }

    @Override
    public int getNumPages() {
        return bufferCache.getNumPages();
    }

    @Override
    public void close() throws HyracksDataException {
        bufferCache.close();
    }

    public void resetCounters() {
        pinCount.set(0);
        unpinCount.set(0);
        readLatchCount.set(0);
        readUnlatchCount.set(0);
        writeLatchCount.set(0);
        writeUnlatchCount.set(0);
        createFileCount.set(0);
        deleteFileCount.set(0);
        openFileCount.set(0);
        closeFileCount.set(0);
    }

    public long getPinCount() {
        return pinCount.get();
    }

    public long getUnpinCount() {
        return unpinCount.get();
    }

    public long getReadLatchCount() {
        return readLatchCount.get();
    }

    public long getReadUnlatchCount() {
        return readUnlatchCount.get();
    }

    public long getWriteLatchCount() {
        return writeLatchCount.get();
    }

    public long getWriteUnlatchCount() {
        return writeUnlatchCount.get();
    }

    public long getCreateFileCount() {
        return createFileCount.get();
    }

    public long getDeleteFileCount() {
        return deleteFileCount.get();
    }

    public long getOpenFileCount() {
        return openFileCount.get();
    }

    public long getCloseFileCount() {
        return closeFileCount.get();
    }

    @Override
    public void flushDirtyPage(ICachedPage page) throws HyracksDataException {
        bufferCache.flushDirtyPage(page);
    }

    @Override
    public void force(int fileId, boolean metadata) throws HyracksDataException {
        bufferCache.force(fileId, metadata);
    }
}
