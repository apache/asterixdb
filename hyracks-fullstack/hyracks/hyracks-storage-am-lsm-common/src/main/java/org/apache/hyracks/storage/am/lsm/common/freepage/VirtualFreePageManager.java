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

package org.apache.hyracks.storage.am.lsm.common.freepage;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageWriteFailureCallback;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class VirtualFreePageManager implements IPageManager {
    private final AtomicInteger currentPageId = new AtomicInteger();
    private final IBufferCache bufferCache;
    private int fileId;

    public VirtualFreePageManager(IBufferCache bufferCache) {
        // We start the currentPageId from 1, because the BTree uses
        // the first page as metadata page, and the second page as root page.
        // (when returning free pages we first increment, then get)
        this.bufferCache = bufferCache;
        currentPageId.set(1);
    }

    @Override
    public int takePage(ITreeIndexMetadataFrame metaFrame) throws HyracksDataException {
        // The very first call returns page id 2 because the BTree uses
        // the first page as metadata page, and the second page as root page.
        return currentPageId.incrementAndGet();
    }

    @Override
    public int takeBlock(ITreeIndexMetadataFrame metaFrame, int count) throws HyracksDataException {
        return currentPageId.getAndUpdate(operand -> operand + count) + 1;
    }

    @Override
    public int getMaxPageId(ITreeIndexMetadataFrame metaFrame) throws HyracksDataException {
        return currentPageId.get();
    }

    @Override
    public ITreeIndexMetadataFrame createMetadataFrame() {
        return null;
    }

    @Override
    public void releasePage(ITreeIndexMetadataFrame metaFrame, int freePage) throws HyracksDataException {
        throw new HyracksDataException("Pages of an in memory index are released through the virtual buffer cache");
    }

    @Override
    public void releaseBlock(ITreeIndexMetadataFrame metaFrame, int startingPage, int count)
            throws HyracksDataException {
        throw new HyracksDataException("Pages of an in memory index are released through the virtual buffer cache");
    }

    @Override
    public int getMetadataPageId() {
        //MD page in a virtual context is always 0, because it is by nature an in-place modification tree
        return 0;
    }

    @Override
    public void open(int fileId) {
        this.fileId = fileId;
    }

    @Override
    public void close(IPageWriteFailureCallback callback) {
        // Method doesn't make sense for this free page manager.
    }

    @Override
    public void init(ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory)
            throws HyracksDataException {
        currentPageId.set(1);
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, 0), true);
        page.acquireWriteLatch();
        page.releaseWriteLatch(false);
        bufferCache.unpin(page);
        page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId.get()), true);
        if (leafFrameFactory != null) {
            page.acquireWriteLatch();
            ITreeIndexFrame leafFrame = leafFrameFactory.createFrame();
            leafFrame.setPage(page);
            leafFrame.initBuffer((byte) 0);
            page.releaseWriteLatch(true);
        }
        bufferCache.unpin(page);
    }

    @Override
    public int getRootPageId() throws HyracksDataException {
        // root index is always 1 for in memory index
        return 1;
    }

    @Override
    public void setRootPageId(int rootPage) throws HyracksDataException {
        // the root of an in memory index will always be 1
    }

    @Override
    public int getBulkLoadLeaf() throws HyracksDataException {
        return 2;
    }

    @Override
    public boolean isEmpty(ITreeIndexFrame frame, int rootPage) throws HyracksDataException {
        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), false);
        rootNode.acquireReadLatch();
        try {
            frame.setPage(rootNode);
            return frame.getLevel() == 0 && frame.getTupleCount() == 0;
        } finally {
            rootNode.releaseReadLatch();
            bufferCache.unpin(rootNode);
        }
    }
}
