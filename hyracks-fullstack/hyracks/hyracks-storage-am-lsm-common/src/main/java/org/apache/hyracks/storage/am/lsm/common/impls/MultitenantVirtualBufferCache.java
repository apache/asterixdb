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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.replication.IIOReplicationManager;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageQueue;
import org.apache.hyracks.storage.common.file.IFileMapManager;

public class MultitenantVirtualBufferCache implements IVirtualBufferCache {
    private static final Logger LOGGER = Logger.getLogger(ExternalIndexHarness.class.getName());

    private final IVirtualBufferCache vbc;
    private int openCount;

    public MultitenantVirtualBufferCache(IVirtualBufferCache virtualBufferCache) {
        this.vbc = virtualBufferCache;
        openCount = 0;
    }

    @Override
    public void createFile(FileReference fileRef) throws HyracksDataException {
        vbc.createFile(fileRef);
    }

    @Override
    public void openFile(int fileId) throws HyracksDataException {
        vbc.openFile(fileId);
    }

    @Override
    public void closeFile(int fileId) throws HyracksDataException {
        vbc.closeFile(fileId);
    }

    @Override
    public void deleteFile(int fileId, boolean flushDirtyPages) throws HyracksDataException {
        vbc.deleteFile(fileId, flushDirtyPages);
    }

    @Override
    public ICachedPage tryPin(long dpid) throws HyracksDataException {
        return vbc.tryPin(dpid);
    }

    @Override
    public ICachedPage pin(long dpid, boolean newPage) throws HyracksDataException {
        return vbc.pin(dpid, newPage);
    }

    @Override
    public void unpin(ICachedPage page) throws HyracksDataException {
        vbc.unpin(page);
    }

    @Override
    public void flushDirtyPage(ICachedPage page) throws HyracksDataException {
        vbc.flushDirtyPage(page);
    }

    @Override
    public void force(int fileId, boolean metadata) throws HyracksDataException {
        vbc.force(fileId, metadata);
    }

    @Override
    public int getPageSize() {
        return vbc.getPageSize();
    }

    public int getPageSizeWithHeader() {
        return vbc.getPageSizeWithHeader();
    }

    @Override
    public int getNumPages() {
        return vbc.getNumPages();
    }

    @Override
    public synchronized void close() throws HyracksDataException {
        --openCount;
        if (openCount == 0) {
            vbc.close();
        }
    }

    @Override
    public synchronized void open() throws HyracksDataException {
        ++openCount;
        if (openCount == 1) {
            vbc.open();
        }
    }

    @Override
    public boolean isFull() {
        return vbc.isFull();
    }

    @Override
    public void reset() {
        vbc.reset();
    }

    @Override
    public IFileMapManager getFileMapProvider() {
        return vbc.getFileMapProvider();
    }

    //These 4 methods are not applicable here
    @Override
    public int createMemFile() throws HyracksDataException {
        throw new UnsupportedOperationException("Virtual Pages are not a valid concept in this context");
    }

    @Override
    public void deleteMemFile(int fileId) throws HyracksDataException {
        throw new UnsupportedOperationException("Virtual Pages are not a valid concept in this context");
    }

    @Override
    public int getNumPagesOfFile(int fileId) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void adviseWontNeed(ICachedPage page) {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.log(Level.INFO, "Calling adviseWontNeed on " + this.getClass().getName()
                    + " makes no sense as this BufferCache cannot evict pages");
        }
    }

    @Override
    public ICachedPage confiscatePage(long dpid) throws HyracksDataException {
        return vbc.confiscatePage(dpid);
    }

    @Override
    public ICachedPage confiscateLargePage(long dpid, int multiplier, int extraBlockPageId)
            throws HyracksDataException {
        return vbc.confiscateLargePage(dpid, multiplier, extraBlockPageId);
    }

    @Override
    public void returnPage(ICachedPage page) {
        vbc.returnPage(page);
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
    public void copyPage(ICachedPage src, ICachedPage dst) {

    }

    @Override
    public void setPageDiskId(ICachedPage page, long dpid) {

    }

    @Override
    public void returnPage(ICachedPage page, boolean reinsert) {

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

    @Override
    public void resizePage(ICachedPage page, int multiplier, IExtraPageBlockHelper extraPageBlockHelper)
            throws HyracksDataException {
        vbc.resizePage(page, multiplier, extraPageBlockHelper);
    }
}
