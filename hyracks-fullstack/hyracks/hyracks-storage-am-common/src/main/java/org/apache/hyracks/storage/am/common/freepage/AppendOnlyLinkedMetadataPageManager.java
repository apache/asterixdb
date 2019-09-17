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
package org.apache.hyracks.storage.am.common.freepage;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrameFactory;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageWriter;
import org.apache.hyracks.storage.common.buffercache.IPageWriteFailureCallback;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;
import org.apache.hyracks.storage.common.compression.file.ICompressedPageWriter;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class AppendOnlyLinkedMetadataPageManager implements IMetadataPageManager {
    private final IBufferCache bufferCache;
    private int metadataPage = IBufferCache.INVALID_PAGEID;
    private int fileId = -1;
    private final ITreeIndexMetadataFrameFactory frameFactory;
    private ICachedPage confiscatedPage;
    private boolean ready = false;

    public AppendOnlyLinkedMetadataPageManager(IBufferCache bufferCache, ITreeIndexMetadataFrameFactory frameFactory) {
        this.bufferCache = bufferCache;
        this.frameFactory = frameFactory;
    }

    @Override
    public void releasePage(ITreeIndexMetadataFrame metaFrame, int freePageNum) throws HyracksDataException {
        ICachedPage metaPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getMetadataPageId()), false);
        metaPage.acquireWriteLatch();
        try {
            metaFrame.setPage(metaPage);
            if (metaFrame.getSpace() > Integer.BYTES) {
                metaFrame.addFreePage(freePageNum);
            } else {
                int newPageNum = metaFrame.getFreePage();
                if (newPageNum < 0) {
                    throw new HyracksDataException(
                            "Inconsistent Meta Page State. It has no space, but it also has no entries.");
                }
                ICachedPage newNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, newPageNum), false);
                newNode.acquireWriteLatch();
                try {
                    int metaMaxPage = metaFrame.getMaxPage();
                    System.arraycopy(metaPage.getBuffer().array(), 0, newNode.getBuffer().array(), 0,
                            metaPage.getBuffer().capacity());
                    metaFrame.init();
                    metaFrame.setNextMetadataPage(newPageNum);
                    metaFrame.setMaxPage(metaMaxPage);
                    metaFrame.addFreePage(freePageNum);
                } finally {
                    newNode.releaseWriteLatch(true);
                    bufferCache.unpin(newNode);
                }
            }
        } finally {
            metaPage.releaseWriteLatch(true);
            bufferCache.unpin(metaPage);
        }
    }

    @Override
    public void releaseBlock(ITreeIndexMetadataFrame metaFrame, int startingPage, int count)
            throws HyracksDataException {
        for (int i = 0; i < count; i++) {
            releasePage(metaFrame, startingPage + i);
        }
    }

    @Override
    public int takePage(ITreeIndexMetadataFrame metaFrame) throws HyracksDataException {
        confiscatedPage.acquireWriteLatch();
        int freePage = IBufferCache.INVALID_PAGEID;
        try {
            metaFrame.setPage(confiscatedPage);
            freePage = metaFrame.getFreePage();
            if (freePage < 0) { // no free page entry on this page
                int nextPage = metaFrame.getNextMetadataPage();
                if (nextPage > 0) { // sibling may have free pages
                    ICachedPage nextNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, nextPage), false);
                    nextNode.acquireWriteLatch();
                    // we copy over the free space entries of nextpage into the
                    // first meta page (metaDataPage)
                    // we need to link the first page properly to the next page
                    // of nextpage
                    try {
                        // remember entries that remain unchanged
                        int maxPage = metaFrame.getMaxPage();
                        // copy entire page (including sibling pointer, free
                        // page entries, and all other info)
                        // after this copy nextPage is considered a free page
                        System.arraycopy(nextNode.getBuffer().array(), 0, confiscatedPage.getBuffer().array(), 0,
                                nextNode.getBuffer().capacity());
                        // reset unchanged entry
                        metaFrame.setMaxPage(maxPage);
                        freePage = metaFrame.getFreePage();
                        // sibling also has no free pages, this "should" not
                        // happen, but we deal with it anyway just to be safe
                        if (freePage < 0) {
                            freePage = nextPage;
                        } else {
                            metaFrame.addFreePage(nextPage);
                        }
                    } finally {
                        nextNode.releaseWriteLatch(true);
                        bufferCache.unpin(nextNode);
                    }
                } else {
                    freePage = metaFrame.getMaxPage();
                    freePage++;
                    metaFrame.setMaxPage(freePage);
                }
            }
        } finally {
            confiscatedPage.releaseWriteLatch(false);
        }
        return freePage;
    }

    @Override
    public int takeBlock(ITreeIndexMetadataFrame metaFrame, int count) throws HyracksDataException {
        int maxPage = metaFrame.getMaxPage();
        metaFrame.setMaxPage(maxPage + count);
        return maxPage + 1;
    }

    @Override
    public int getMaxPageId(ITreeIndexMetadataFrame metaFrame) throws HyracksDataException {
        ICachedPage metaNode;
        if (confiscatedPage == null) {
            int mdPage = getMetadataPageId();
            if (mdPage < 0) {
                return IBufferCache.INVALID_PAGEID;
            }
            metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, mdPage), false);
        } else {
            metaNode = confiscatedPage;
        }
        metaNode.acquireReadLatch();
        int maxPage = -1;
        try {
            metaFrame.setPage(metaNode);
            maxPage = metaFrame.getMaxPage();
        } finally {
            metaNode.releaseReadLatch();
            if (confiscatedPage == null) {
                bufferCache.unpin(metaNode);
            }
        }
        return maxPage;
    }

    @Override
    public void init(ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory)
            throws HyracksDataException {
        // an initialized append only tree is always completely empty with size = 0, hence, this operation is a No Op
    }

    @Override
    public ITreeIndexMetadataFrame createMetadataFrame() {
        return frameFactory.createFrame();
    }

    @Override
    public void open(int fileId) throws HyracksDataException {
        this.fileId = fileId;
        // get the number of pages of the file
        int pages = bufferCache.getNumPagesOfFile(fileId);
        //if there are no pages in the file yet, we're just initializing
        if (pages == 0) {
            if (confiscatedPage != null) {
                throw new HyracksDataException("Metadata Page Manager is already initialized");
            }
            ITreeIndexMetadataFrame metaFrame = createMetadataFrame();
            ICachedPage metaNode = bufferCache.confiscatePage(BufferCache.INVALID_DPID);
            try {
                metaFrame.setPage(metaNode);
                metaFrame.init();
                metaFrame.setMaxPage(-1);
            } finally {
                confiscatedPage = metaNode;
            }
        }
    }

    @Override
    public void close(IPageWriteFailureCallback failureCallback) throws HyracksDataException {
        if (ready) {
            IFIFOPageWriter pageWriter = bufferCache.createFIFOWriter(NoOpPageWriteCallback.INSTANCE, failureCallback);
            ITreeIndexMetadataFrame metaFrame = frameFactory.createFrame();
            confiscatedPage.acquireWriteLatch();
            try {
                metaFrame.setPage(confiscatedPage);
                metaFrame.setValid(true);
            } finally {
                confiscatedPage.releaseWriteLatch(false);
            }
            int finalMetaPage = getMaxPageId(metaFrame) + 1;
            confiscatedPage.setDiskPageId(BufferedFileHandle.getDiskPageId(fileId, finalMetaPage));
            final ICompressedPageWriter compressedPageWriter = bufferCache.getCompressedPageWriter(fileId);
            compressedPageWriter.prepareWrite(confiscatedPage);
            // WARNING: flushing the metadata page should be done after releasing the write latch; otherwise, the page
            // won't be flushed to disk because it won't be dirty until the write latch has been released.
            pageWriter.write(confiscatedPage);
            compressedPageWriter.endWriting();
            metadataPage = getMetadataPageId();
            ready = false;
        } else if (confiscatedPage != null) {
            bufferCache.returnPage(confiscatedPage, false);
        }
        confiscatedPage = null;
    }

    /**
     * For storage on append-only media (such as HDFS), the meta data page has to be written last.
     * However, some implementations still write the meta data to the front. To deal with this as well
     * as to provide downward compatibility, this method tries to find the meta data page first in the
     * last and then in the first page of the file.
     *
     * @return The Id of the page holding the meta data
     * @throws HyracksDataException
     */
    @Override
    public int getMetadataPageId() throws HyracksDataException {
        if (metadataPage != IBufferCache.INVALID_PAGEID) {
            return metadataPage;
        }
        int pages = bufferCache.getNumPagesOfFile(fileId);
        if (pages == 0) {
            //At least there are 2 pages to consider the index is not empty
            return IBufferCache.INVALID_PAGEID;
        }
        metadataPage = pages - 1;
        return metadataPage;
    }

    @Override
    public boolean isEmpty(ITreeIndexFrame frame, int rootPage) throws HyracksDataException {
        return bufferCache.getNumPagesOfFile(fileId) <= AbstractTreeIndex.MINIMAL_TREE_PAGE_COUNT;
    }

    @Override
    public void setRootPageId(int rootPage) throws HyracksDataException {
        ITreeIndexMetadataFrame metaFrame = frameFactory.createFrame();
        confiscatedPage.acquireWriteLatch();
        try {
            metaFrame.setPage(confiscatedPage);
            metaFrame.setRootPageId(rootPage);
        } finally {
            confiscatedPage.releaseWriteLatch(false);
        }
        ready = true;
    }

    @Override
    public int getRootPageId() throws HyracksDataException {
        ICachedPage metaNode;
        if (confiscatedPage == null) {
            metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getMetadataPageId()), false);
        } else {
            metaNode = confiscatedPage;
        }
        ITreeIndexMetadataFrame metaFrame = frameFactory.createFrame();
        metaNode.acquireReadLatch();
        try {
            metaFrame.setPage(metaNode);
            return metaFrame.getRootPageId();
        } finally {
            metaNode.releaseReadLatch();
            if (confiscatedPage == null) {
                bufferCache.unpin(metaNode);
            }
        }
    }

    @Override
    public int getBulkLoadLeaf() throws HyracksDataException {
        return 0;
    }

    @Override
    public void put(ITreeIndexMetadataFrame frame, IValueReference key, IValueReference value)
            throws HyracksDataException {
        if (confiscatedPage == null) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_WRITE_AFTER_FLUSH_ATTEMPT);
        }
        confiscatedPage.acquireWriteLatch();
        try {
            frame.setPage(confiscatedPage);
            frame.put(key, value);
        } finally {
            confiscatedPage.releaseWriteLatch(false);
        }
    }

    private ICachedPage pinPage() throws HyracksDataException {
        return confiscatedPage == null
                ? bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getMetadataPageId()), false)
                : confiscatedPage;
    }

    private void unpinPage(ICachedPage page) throws HyracksDataException {
        if (confiscatedPage == null) {
            bufferCache.unpin(page);
        }
    }

    @Override
    public void get(ITreeIndexMetadataFrame frame, IValueReference key, IPointable value) throws HyracksDataException {
        ICachedPage page = pinPage();
        page.acquireReadLatch();
        try {
            frame.setPage(page);
            frame.get(key, value);
        } finally {
            page.releaseReadLatch();
            unpinPage(page);
        }
    }

    @Override
    public long getFileOffset(ITreeIndexMetadataFrame frame, IValueReference key) throws HyracksDataException {
        int pageId = getMetadataPageId();
        if (pageId != IBufferCache.INVALID_PAGEID) {
            ICachedPage page = pinPage();
            page.acquireReadLatch();
            try {
                frame.setPage(page);
                int inPageOffset = frame.getOffset(key);
                return inPageOffset >= 0 ? ((long) pageId * bufferCache.getPageSizeWithHeader()) + frame.getOffset(key)
                        + IBufferCache.RESERVED_HEADER_BYTES : -1L;
            } finally {
                page.releaseReadLatch();
                unpinPage(page);
            }
        }
        return -1L;
    }
}
