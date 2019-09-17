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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.replication.IIOReplicationManager;

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
    public int createFile(FileReference fileRef) throws HyracksDataException {
        int fileId = bufferCache.createFile(fileRef);
        createFileCount.addAndGet(1);
        return fileId;
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
    public void deleteFile(int fileId) throws HyracksDataException {
        bufferCache.deleteFile(fileId);
        deleteFileCount.addAndGet(1);
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
    public int getPageSizeWithHeader() {
        return bufferCache.getPageSizeWithHeader();
    }

    @Override
    public int getPageBudget() {
        return bufferCache.getPageBudget();
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
    public void flush(ICachedPage page) throws HyracksDataException {
        bufferCache.flush(page);
    }

    @Override
    public void force(int fileId, boolean metadata) throws HyracksDataException {
        bufferCache.force(fileId, metadata);
    }

    @Override
    public int getNumPagesOfFile(int fileId) throws HyracksDataException {
        return bufferCache.getNumPagesOfFile(fileId);
    }

    @Override
    public ICachedPage confiscatePage(long dpid) throws HyracksDataException {
        return bufferCache.confiscatePage(dpid);
    }

    @Override
    public ICachedPage confiscateLargePage(long dpid, int multiplier, int extraBlockPageId)
            throws HyracksDataException {
        return bufferCache.confiscateLargePage(dpid, multiplier, extraBlockPageId);
    }

    @Override
    public void returnPage(ICachedPage page) {
        bufferCache.returnPage(page);
    }

    @Override
    public IFIFOPageWriter createFIFOWriter(IPageWriteCallback callback, IPageWriteFailureCallback failureCallback) {
        return bufferCache.createFIFOWriter(callback, failureCallback);
    }

    @Override
    public void returnPage(ICachedPage page, boolean reinsert) {
        // TODO Auto-generated method stub

    }

    @Override
    public int getFileReferenceCount(int fileId) {
        return bufferCache.getFileReferenceCount(fileId);
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
        bufferCache.purgeHandle(fileId);
    }

    @Override
    public void resizePage(ICachedPage page, int multiplier, IExtraPageBlockHelper extraPageBlockHelper)
            throws HyracksDataException {
        bufferCache.resizePage(page, multiplier, extraPageBlockHelper);
    }

    @Override
    public int openFile(FileReference fileRef) throws HyracksDataException {
        openFileCount.incrementAndGet();
        return bufferCache.openFile(fileRef);
    }

    @Override
    public void deleteFile(FileReference file) throws HyracksDataException {
        deleteFileCount.incrementAndGet();
        bufferCache.deleteFile(file);
    }

    @Override
    public void closeFileIfOpen(FileReference fileRef) {
        bufferCache.closeFileIfOpen(fileRef);
    }

}
