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
import org.apache.hyracks.storage.am.common.api.IMetaDataPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrame;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageQueue;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class LinkedMetaDataPageManager implements IMetaDataPageManager {

    private static final byte META_PAGE_LEVEL_INDICATOR = -1;
    private static final byte FREE_PAGE_LEVEL_INDICATOR = -2;
    public static final int NO_FILTER_IN_PLACE = -1;
    public static final int NO_FILTER_APPEND_ONLY = -2;
    private final IBufferCache bufferCache;
    private int headPage = IBufferCache.INVALID_PAGEID;
    private int fileId = -1;
    private final ITreeIndexMetaDataFrameFactory metaDataFrameFactory;
    private boolean appendOnly = false;
    ICachedPage confiscatedMetaNode;
    ICachedPage filterPage;

    public LinkedMetaDataPageManager(IBufferCache bufferCache, ITreeIndexMetaDataFrameFactory metaDataFrameFactory) {
        this.bufferCache = bufferCache;
        this.metaDataFrameFactory = metaDataFrameFactory;
    }

    @Override
    public void addFreePage(ITreeIndexMetaDataFrame metaFrame, int freePage) throws HyracksDataException {
        ICachedPage metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getFirstMetadataPage()), false);
        metaNode.acquireWriteLatch();

        try {
            metaFrame.setPage(metaNode);

            if (metaFrame.hasSpace()) {
                metaFrame.addFreePage(freePage);
            } else {
                // allocate a new page in the chain of meta pages
                int newPage = metaFrame.getFreePage();
                if (newPage < 0) {
                    throw new HyracksDataException(
                              "Inconsistent Meta Page State. It has no space, but it also has no entries.");
                }

                ICachedPage newNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, newPage), false);
                newNode.acquireWriteLatch();

                try {
                    int metaMaxPage = metaFrame.getMaxPage();

                    // copy metaDataPage to newNode
                    System.arraycopy(metaNode.getBuffer().array(), 0, newNode.getBuffer().array(), 0,
                            metaNode.getBuffer().capacity());

                    metaFrame.initBuffer(META_PAGE_LEVEL_INDICATOR);
                    metaFrame.setNextPage(newPage);
                    metaFrame.setMaxPage(metaMaxPage);
                    metaFrame.addFreePage(freePage);
                } finally {
                    newNode.releaseWriteLatch(true);
                    bufferCache.unpin(newNode);
                }
            }
        } finally {
            metaNode.releaseWriteLatch(true);
            bufferCache.unpin(metaNode);
        }
    }

    @Override
    public void addFreePageBlock(ITreeIndexMetaDataFrame metaFrame, int startingPage, int count)
            throws HyracksDataException {
        for (int i = 0; i < count; i++) {
            addFreePage(metaFrame, startingPage + i);
        }
    }

    @Override
    public int getFreePage(ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {
        ICachedPage metaNode;
        if (!appendOnly) {
            metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getFirstMetadataPage()), false);
        } else {
            metaNode = confiscatedMetaNode;
        }

        metaNode.acquireWriteLatch();

        int freePage = IBufferCache.INVALID_PAGEID;
        try {
            metaFrame.setPage(metaNode);
            freePage = metaFrame.getFreePage();
            if (freePage < 0) { // no free page entry on this page
                int nextPage = metaFrame.getNextPage();
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
            if (!appendOnly) {
                metaNode.releaseWriteLatch(true);
                bufferCache.unpin(metaNode);
            } else {
                metaNode.releaseWriteLatch(false);
            }
        }

        return freePage;
    }

    @Override
    public int getFreePageBlock(ITreeIndexMetaDataFrame metaFrame, int count) throws HyracksDataException {
        int maxPage = metaFrame.getMaxPage();
        metaFrame.setMaxPage(maxPage + count);
        return maxPage + 1;
    }

    @Override
    public int getMaxPage(ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {
        ICachedPage metaNode;
        if (!appendOnly || confiscatedMetaNode == null) {
            int mdPage = getFirstMetadataPage();
            if (mdPage < 0) {
                return IBufferCache.INVALID_PAGEID;
            }
            metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, mdPage), false);
        } else {
            metaNode = confiscatedMetaNode;
        }
        metaNode.acquireReadLatch();
        int maxPage = -1;
        try {
            metaFrame.setPage(metaNode);
            maxPage = metaFrame.getMaxPage();
        } finally {
            metaNode.releaseReadLatch();
            if (!appendOnly || confiscatedMetaNode == null) {
                bufferCache.unpin(metaNode);
            }
        }
        return maxPage;
    }

    @Override
    public void setFilterPageId(int filterPageId) throws HyracksDataException {
        ICachedPage metaNode;
        if (!appendOnly) {
            int mdPage = getFirstMetadataPage();
            if (mdPage < 0) {
                return;
            }
            metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, mdPage), false);
        } else {
            metaNode = confiscatedMetaNode;
        }
        ITreeIndexMetaDataFrame metaFrame = metaDataFrameFactory.createFrame();
        metaNode.acquireWriteLatch();
        try {
            metaFrame.setPage(metaNode);
            metaFrame.setLSMComponentFilterPageId(filterPageId);
        } finally {
            if (!appendOnly) {
                metaNode.releaseWriteLatch(true);
                bufferCache.unpin(metaNode);
            } else {
                metaNode.releaseWriteLatch(false);
            }
        }
    }

    @Override
    public int getFilterPageId() throws HyracksDataException {
        ICachedPage metaNode;
        int filterPageId = NO_FILTER_IN_PLACE;
        if (!appendOnly || confiscatedMetaNode == null) {
            metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getFirstMetadataPage()), false);
        } else {
            metaNode = confiscatedMetaNode;
        }
        ITreeIndexMetaDataFrame metaFrame = metaDataFrameFactory.createFrame();
        metaNode.acquireReadLatch();
        try {
            metaFrame.setPage(metaNode);
            filterPageId = metaFrame.getLSMComponentFilterPageId();
            if (appendOnly && filterPageId == -1) {
                //hint to filter manager that we are in append-only mode
                filterPageId = NO_FILTER_APPEND_ONLY;
            }
        } finally {
            metaNode.releaseReadLatch();
            if (!appendOnly || confiscatedMetaNode == null) {
                bufferCache.unpin(metaNode);
            }
        }
        return filterPageId;
    }

    @Override
    public void init(ITreeIndexMetaDataFrame metaFrame, int currentMaxPage) throws HyracksDataException {
        // initialize meta data page
        int metaPage = getFirstMetadataPage();
        if (metaPage == IBufferCache.INVALID_PAGEID) {
            throw new HyracksDataException("No valid metadata found in this file.");
        }
        ICachedPage metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getFirstMetadataPage()), true);

        metaNode.acquireWriteLatch();
        try {
            metaFrame.setPage(metaNode);
            metaFrame.initBuffer(META_PAGE_LEVEL_INDICATOR);
            metaFrame.setMaxPage(currentMaxPage);
        } finally {
            metaNode.releaseWriteLatch(true);
            bufferCache.unpin(metaNode);
        }
    }

    @Override
    public void init(ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {
        if (confiscatedMetaNode != null) { // don't init twice
            return;
        }
        ICachedPage metaNode = bufferCache.confiscatePage(BufferCache.INVALID_DPID);
        try {
            metaFrame.setPage(metaNode);
            metaFrame.initBuffer(META_PAGE_LEVEL_INDICATOR);
            metaFrame.setMaxPage(-1);
        } finally {
            confiscatedMetaNode = metaNode;
            appendOnly = true;
        }
    }

    @Override
    public ITreeIndexMetaDataFrameFactory getMetaDataFrameFactory() {
        return metaDataFrameFactory;
    }

    @Override
    public byte getFreePageLevelIndicator() {
        return FREE_PAGE_LEVEL_INDICATOR;
    }

    @Override
    public byte getMetaPageLevelIndicator() {
        return META_PAGE_LEVEL_INDICATOR;
    }

    @Override
    public boolean isFreePage(ITreeIndexMetaDataFrame metaFrame) {
        return metaFrame.getLevel() == FREE_PAGE_LEVEL_INDICATOR;
    }

    @Override
    public boolean isMetaPage(ITreeIndexMetaDataFrame metaFrame) {
        return metaFrame.getLevel() == META_PAGE_LEVEL_INDICATOR;
    }

    @Override
    public void open(int fileId) {
        this.fileId = fileId;
    }

    @Override
    public void close() throws HyracksDataException {
        if (appendOnly && fileId >= 0 && confiscatedMetaNode != null) {
            IFIFOPageQueue queue = bufferCache.createFIFOQueue();
            writeFilterPage(queue);
            ITreeIndexMetaDataFrame metaFrame = metaDataFrameFactory.createFrame();
            metaFrame.setPage(confiscatedMetaNode);
            metaFrame.setValid(true);
            int finalMetaPage = getMaxPage(metaFrame) + 1;
            bufferCache.setPageDiskId(confiscatedMetaNode, BufferedFileHandle.getDiskPageId(fileId, finalMetaPage));
            queue.put(confiscatedMetaNode);
            bufferCache.finishQueue();
            confiscatedMetaNode = null;
        }
    }

    private void writeFilterPage(IFIFOPageQueue queue) throws HyracksDataException {
        if (filterPage != null) {
            ITreeIndexMetaDataFrame metaFrame = metaDataFrameFactory.createFrame();
            metaFrame.setPage(confiscatedMetaNode);
            int finalFilterPage = getFreePage(metaFrame);
            setFilterPageId(finalFilterPage);
            bufferCache.setPageDiskId(filterPage, BufferedFileHandle.getDiskPageId(fileId, finalFilterPage));
            queue.put(filterPage);
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
    public int getFirstMetadataPage() throws HyracksDataException {
        if (headPage != IBufferCache.INVALID_PAGEID) {
            return headPage;
        }

        ITreeIndexMetaDataFrame metaFrame = metaDataFrameFactory.createFrame();

        int pages = bufferCache.getNumPagesOfFile(fileId);
        //if there are no pages in the file yet, we're just initializing
        if (pages == 0) {
            return 0;
        }
        //look at the front (modify in-place index)
        int page = 0;
        ICachedPage metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, page), false);
        try {
            metaNode.acquireReadLatch();
            metaFrame.setPage(metaNode);

            if (isMetaPage(metaFrame)) {
                headPage = page;
                return headPage;
            }
        } finally {
            metaNode.releaseReadLatch();
            bufferCache.unpin(metaNode);
        }
        //otherwise, look at the back. (append-only index)
        page = pages - 1 > 0 ? pages - 1 : 0;
        metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, page), false);
        try {
            metaNode.acquireReadLatch();
            metaFrame.setPage(metaNode);

            if (isMetaPage(metaFrame)) {
                headPage = page;
                return headPage;
            }
        } finally {
            metaNode.releaseReadLatch();
            bufferCache.unpin(metaNode);
        }
        //if we find nothing, this isn't a tree (or isn't one yet).
        if (pages > 0) {
            return IBufferCache.INVALID_PAGEID;
        } else {
            return 0;
        }
    }

    @Override
    public long getLSN() throws HyracksDataException {
        ICachedPage metaNode;
        if (!appendOnly || confiscatedMetaNode == null) {
            metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getFirstMetadataPage()), false);
        } else {
            metaNode = confiscatedMetaNode;
        }
        ITreeIndexMetaDataFrame metaFrame = metaDataFrameFactory.createFrame();
        metaNode.acquireReadLatch();
        try {
            metaFrame.setPage(metaNode);
            return metaFrame.getLSN();
        } finally {
            metaNode.releaseReadLatch();
            if (!appendOnly || confiscatedMetaNode == null) {
                bufferCache.unpin(metaNode);
            }
        }
    }

    @Override
    public void setLSN(long lsn) throws HyracksDataException {
        ICachedPage metaNode;
        if (!appendOnly) {
            metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getFirstMetadataPage()), false);
        } else {
            metaNode = confiscatedMetaNode;
        }
        ITreeIndexMetaDataFrame metaFrame = metaDataFrameFactory.createFrame();
        metaNode.acquireWriteLatch();
        try {
            metaFrame.setPage(metaNode);
            metaFrame.setLSN(lsn);
        } finally {
            if (!appendOnly) {
                metaNode.releaseWriteLatch(true);
                bufferCache.unpin(metaNode);
            } else {
                metaNode.releaseWriteLatch(false);
            }
        }
    }

    @Override
    public void setFilterPage(ICachedPage filterPage) {
        this.filterPage = filterPage;
    }

    @Override
    public ICachedPage getFilterPage() {
        return this.filterPage;
    }

    @Override
    public boolean appendOnlyMode() {
        return appendOnly;
    }

    @Override
    public long getLSNOffset() throws HyracksDataException {
        int metadataPageNum = getFirstMetadataPage();
        if (metadataPageNum != IBufferCache.INVALID_PAGEID) {
            return ((long)metadataPageNum * bufferCache.getPageSize()) + LIFOMetaDataFrame.LSN_OFFSET;
        }
        return IMetaDataPageManager.INVALID_LSN_OFFSET;
    }

    @Override
    public long getLastMarkerLSN() throws HyracksDataException {
        ICachedPage metaNode;
        if (!appendOnly || (appendOnly && confiscatedMetaNode == null)) {
            metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getFirstMetadataPage()), false);
        } else {
            metaNode = confiscatedMetaNode;
        }
        ITreeIndexMetaDataFrame metaFrame = metaDataFrameFactory.createFrame();
        metaNode.acquireReadLatch();
        try {
            metaFrame.setPage(metaNode);
            return metaFrame.getLastMarkerLSN();
        } finally {
            metaNode.releaseReadLatch();
            if (!appendOnly || (appendOnly && confiscatedMetaNode == null)) {
                bufferCache.unpin(metaNode);
            }
        }
    }
}
