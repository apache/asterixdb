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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrame;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class LinkedMetaDataPageManager implements IMetadataPageManager {
    private final IBufferCache bufferCache;
    private int fileId = -1;
    private final ITreeIndexMetaDataFrameFactory frameFactory;
    private boolean ready = false;

    public LinkedMetaDataPageManager(IBufferCache bufferCache, ITreeIndexMetaDataFrameFactory frameFactory) {
        this.bufferCache = bufferCache;
        this.frameFactory = frameFactory;
    }

    @Override
    public void releasePage(ITreeIndexMetaDataFrame metaFrame, int freePageNum) throws HyracksDataException {
        // Get the metadata node
        ICachedPage metaPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getMetadataPageId()), false);
        metaPage.acquireWriteLatch();
        try {
            metaFrame.setPage(metaPage);

            if (metaFrame.hasSpace()) {
                metaFrame.addFreePage(freePageNum);
            } else {
                // allocate a new page in the chain of meta pages
                int newPageNum = metaFrame.getFreePage();
                if (newPageNum < 0) {
                    throw new HyracksDataException(
                            "Inconsistent Meta Page State. It has no space, but it also has no entries.");
                }

                ICachedPage newNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, newPageNum), false);
                newNode.acquireWriteLatch();

                try {
                    int metaMaxPage = metaFrame.getMaxPage();

                    // copy metaDataPage to newNode
                    System.arraycopy(metaPage.getBuffer().array(), 0, newNode.getBuffer().array(), 0,
                            metaPage.getBuffer().capacity());

                    metaFrame.initBuffer();
                    metaFrame.setNextPage(newPageNum);
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
    public void releaseBlock(ITreeIndexMetaDataFrame metaFrame, int startingPage, int count)
            throws HyracksDataException {
        for (int i = 0; i < count; i++) {
            releasePage(metaFrame, startingPage + i);
        }
    }

    @Override
    public int takePage(ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {
        ICachedPage metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getMetadataPageId()), false);

        metaNode.acquireWriteLatch();

        int freePage = IBufferCache.INVALID_PAGEID;
        try {
            metaFrame.setPage(metaNode);
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
                        System.arraycopy(nextNode.getBuffer().array(), 0, metaNode.getBuffer().array(), 0,
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
            metaNode.releaseWriteLatch(true);
            bufferCache.unpin(metaNode);
        }

        return freePage;
    }

    @Override
    public int takeBlock(ITreeIndexMetaDataFrame metaFrame, int count) throws HyracksDataException {
        int maxPage = metaFrame.getMaxPage();
        metaFrame.setMaxPage(maxPage + count);
        return maxPage + 1;
    }

    @Override
    public int getMaxPageId(ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {
        ICachedPage metaNode;
        int mdPage = getMetadataPageId();
        if (mdPage < 0) {
            return IBufferCache.INVALID_PAGEID;
        }
        metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, mdPage), false);

        metaNode.acquireReadLatch();
        int maxPage = -1;
        try {
            metaFrame.setPage(metaNode);
            maxPage = metaFrame.getMaxPage();
        } finally {
            metaNode.releaseReadLatch();
            bufferCache.unpin(metaNode);
        }
        return maxPage;
    }

    @Override
    public void setFilterPageId(int filterPageId) throws HyracksDataException {
        ICachedPage metaNode;
        int mdPage = getMetadataPageId();
        if (mdPage < 0) {
            return;
        }
        metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, mdPage), false);
        ITreeIndexMetaDataFrame metaFrame = frameFactory.createFrame();
        metaNode.acquireWriteLatch();
        try {
            metaFrame.setPage(metaNode);
            metaFrame.setLSMComponentFilterPageId(filterPageId);
        } finally {
            metaNode.releaseWriteLatch(true);
            bufferCache.unpin(metaNode);
        }
    }

    @Override
    public int getFilterPageId() throws HyracksDataException {
        ICachedPage metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getMetadataPageId()), false);
        ITreeIndexMetaDataFrame metaFrame = frameFactory.createFrame();
        metaNode.acquireReadLatch();
        try {
            metaFrame.setPage(metaNode);
            return metaFrame.getLSMComponentFilterPageId();
        } finally {
            metaNode.releaseReadLatch();
            bufferCache.unpin(metaNode);
        }
    }

    @Override
    public void init(ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory)
            throws HyracksDataException {
        // initialize meta data page
        ITreeIndexMetaDataFrame metaFrame = createMetadataFrame();
        int metaPage = getMetadataPageId();
        if (metaPage == IBufferCache.INVALID_PAGEID) {
            throw new HyracksDataException("No valid metadata found in this file.");
        }
        ICachedPage metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getMetadataPageId()), true);
        metaNode.acquireWriteLatch();
        try {
            metaFrame.setPage(metaNode);
            metaFrame.initBuffer();
            metaFrame.setRootPageNumber(1);
            metaFrame.setMaxPage(1);
        } finally {
            metaNode.releaseWriteLatch(true);
            bufferCache.flushDirtyPage(metaNode);
            bufferCache.unpin(metaNode);
        }
        int rootPage = getRootPageId();
        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), true);
        rootNode.acquireWriteLatch();
        try {
            ITreeIndexFrame leafFrame = leafFrameFactory.createFrame();
            leafFrame.setPage(rootNode);
            leafFrame.initBuffer((byte) 0);
        } finally {
            rootNode.releaseWriteLatch(true);
            bufferCache.flushDirtyPage(rootNode);
            bufferCache.unpin(rootNode);
        }
    }

    @Override
    public ITreeIndexMetaDataFrame createMetadataFrame() {
        return frameFactory.createFrame();
    }

    @Override
    public void open(int fileId) {
        this.fileId = fileId;
    }

    @Override
    public void setRootPageId(int rootPage) throws HyracksDataException {
        ICachedPage metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getMetadataPageId()), false);
        ITreeIndexMetaDataFrame metaFrame = frameFactory.createFrame();
        metaNode.acquireWriteLatch();
        try {
            metaFrame.setPage(metaNode);
            metaFrame.setRootPageNumber(rootPage);
        } finally {
            metaNode.releaseWriteLatch(true);
            bufferCache.unpin(metaNode);
            ready = true;
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (ready) {
            ICachedPage metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getMetadataPageId()),
                    false);
            ITreeIndexMetaDataFrame metaFrame = frameFactory.createFrame();
            metaNode.acquireWriteLatch();
            try {
                metaFrame.setPage(metaNode);
                metaFrame.setValid(true);
            } finally {
                metaNode.releaseWriteLatch(true);
                bufferCache.flushDirtyPage(metaNode);
                bufferCache.unpin(metaNode);
                ready = true;
            }
            ready = false;
        }
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
        return 0;
    }

    @Override
    public long getLSN() throws HyracksDataException {
        ICachedPage metaNode;
        metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getMetadataPageId()), false);
        ITreeIndexMetaDataFrame metaFrame = frameFactory.createFrame();
        metaNode.acquireReadLatch();
        try {
            metaFrame.setPage(metaNode);
            return metaFrame.getLSN();
        } finally {
            metaNode.releaseReadLatch();
            bufferCache.unpin(metaNode);
        }
    }

    @Override
    public void setLSN(long lsn) throws HyracksDataException {
        ICachedPage metaNode;
        metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getMetadataPageId()), false);
        ITreeIndexMetaDataFrame metaFrame = frameFactory.createFrame();
        metaNode.acquireWriteLatch();
        try {
            metaFrame.setPage(metaNode);
            metaFrame.setLSN(lsn);
        } finally {
            metaNode.releaseWriteLatch(true);
            bufferCache.unpin(metaNode);
        }
    }

    @Override
    public void setFilterPage(ICachedPage filterPage) throws HyracksDataException {
        filterPage.releaseWriteLatch(true);
        bufferCache.unpin(filterPage);
        bufferCache.flushDirtyPage(filterPage);
    }

    @Override
    public ICachedPage getFilterPage() throws HyracksDataException {
        ITreeIndexMetaDataFrame metadataFrame = frameFactory.createFrame();
        int metaPageId = getMetadataPageId();
        ICachedPage metadataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, metaPageId), false);
        metadataPage.acquireWriteLatch();
        int filterPageId;
        try {
            metadataFrame.setPage(metadataPage);
            filterPageId = takePage(metadataFrame);
            metadataFrame.setLSMComponentFilterPageId(filterPageId);
        } finally {
            metadataPage.releaseWriteLatch(true);
            bufferCache.unpin(metadataPage);
        }
        ICachedPage filterPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, filterPageId), true);
        filterPage.acquireWriteLatch();
        return filterPage;
    }

    @Override
    public long getLSNOffset() throws HyracksDataException {
        int metadataPageNum = getMetadataPageId();
        if (metadataPageNum != IBufferCache.INVALID_PAGEID) {
            return ((long) metadataPageNum * bufferCache.getPageSizeWithHeader()) + LIFOMetaDataFrame.LSN_OFFSET;
        }
        return IMetadataPageManager.Constants.INVALID_LSN_OFFSET;
    }

    @Override
    public long getLastMarkerLSN() throws HyracksDataException {
        ICachedPage metaNode;
        metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getMetadataPageId()), false);
        ITreeIndexMetaDataFrame metaFrame = frameFactory.createFrame();
        metaNode.acquireReadLatch();
        try {
            metaFrame.setPage(metaNode);
            return metaFrame.getLastMarkerLSN();
        } finally {
            metaNode.releaseReadLatch();
            bufferCache.unpin(metaNode);
        }
    }

    @Override
    public int getRootPageId() throws HyracksDataException {
        ICachedPage metaNode;
        metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getMetadataPageId()), false);
        ITreeIndexMetaDataFrame metaFrame = frameFactory.createFrame();
        metaNode.acquireReadLatch();
        try {
            metaFrame.setPage(metaNode);
            return metaFrame.getRootPageNumber();
        } finally {
            metaNode.releaseReadLatch();
            bufferCache.unpin(metaNode);
        }
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
            if (frame.getLevel() == 0 && frame.getTupleCount() == 0) {
                return true;
            } else {
                return false;
            }
        } finally {
            rootNode.releaseReadLatch();
            bufferCache.unpin(rootNode);
        }
    }
}
