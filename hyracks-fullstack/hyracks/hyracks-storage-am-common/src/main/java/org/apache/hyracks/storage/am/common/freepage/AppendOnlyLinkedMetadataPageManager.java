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

import java.util.ArrayList;
import java.util.List;

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
import org.apache.hyracks.storage.common.buffercache.context.write.DefaultBufferCacheWriteContext;
import org.apache.hyracks.storage.common.compression.file.ICompressedPageWriter;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class AppendOnlyLinkedMetadataPageManager implements IMetadataPageManager {
    private final IBufferCache bufferCache;
    private final ITreeIndexMetadataFrameFactory frameFactory;
    private final List<ICachedPage> metadataPages;
    private int metadataPage = IBufferCache.INVALID_PAGEID;
    private int fileId = -1;
    private ICachedPage currentPage;
    private ICachedPage firstPage;
    private boolean ready = false;

    public AppendOnlyLinkedMetadataPageManager(IBufferCache bufferCache, ITreeIndexMetadataFrameFactory frameFactory) {
        this.bufferCache = bufferCache;
        this.frameFactory = frameFactory;
        metadataPages = new ArrayList<>();
    }

    @Override
    public void releasePage(ITreeIndexMetadataFrame metaFrame, int freePageNum) {
        throw new IllegalAccessError("On-disk pages must be immutable");
    }

    @Override
    public void releaseBlock(ITreeIndexMetadataFrame metaFrame, int startingPage, int count) {
        throw new IllegalAccessError("On-disk pages must be immutable");
    }

    @Override
    public int takePage(ITreeIndexMetadataFrame metaFrame) throws HyracksDataException {
        metaFrame.setPage(firstPage);
        int maxPage = metaFrame.getMaxPage() + 1;
        metaFrame.setMaxPage(maxPage);
        return maxPage;
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
        if (firstPage == null) {
            int mdPage = getMetadataPageId();
            if (mdPage < 0) {
                return IBufferCache.INVALID_PAGEID;
            }
            metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, mdPage));
        } else {
            metaNode = firstPage;
        }

        int maxPage = -1;
        try {
            metaFrame.setPage(metaNode);
            maxPage = metaFrame.getMaxPage();
        } finally {
            if (firstPage == null) {
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
            if (firstPage != null) {
                throw new HyracksDataException("Metadata Page Manager is already initialized");
            }
            ITreeIndexMetadataFrame metaFrame = createMetadataFrame();
            // First to confiscate
            confiscateNext(metaFrame);
            firstPage = currentPage;
            metaFrame.setMaxPage(-1);
        }
    }

    @Override
    public void close(IPageWriteFailureCallback failureCallback) throws HyracksDataException {
        if (ready) {
            persist(failureCallback);
            metadataPage = getMetadataPageId();
            ready = false;
        } else if (!metadataPages.isEmpty()) {
            for (ICachedPage page : metadataPages) {
                bufferCache.returnPage(page, false);
            }
        }
        currentPage = null;
        firstPage = null;
        metadataPages.clear();
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
        metaFrame.setPage(firstPage);
        metaFrame.setRootPageId(rootPage);
        ready = true;
    }

    @Override
    public int getRootPageId() throws HyracksDataException {
        ICachedPage metaNode;
        if (firstPage == null) {
            metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, getMetadataPageId()));
        } else {
            metaNode = firstPage;
        }
        ITreeIndexMetadataFrame metaFrame = frameFactory.createFrame();

        try {
            metaFrame.setPage(metaNode);
            return metaFrame.getRootPageId();
        } finally {
            if (firstPage == null) {
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
        if (currentPage == null) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_WRITE_AFTER_FLUSH_ATTEMPT);
        }

        frame.setPage(currentPage);

        if (frame.getSpace() < key.getLength() + value.getLength() + frame.getKeyValueStorageOverhead()) {
            // If there's no space, confiscate an extra page
            confiscateNext(frame);
        }

        frame.put(key, value);
        if (frame.getSpace() == 0) {
            /*
             * Most likely a user is writing chunks, confiscate a new page so the next call to
             * getFreeSpace() will not return 0.
             */
            confiscateNext(frame);
        }
    }

    @Override
    public boolean get(ITreeIndexMetadataFrame frame, IValueReference key, IPointable value)
            throws HyracksDataException {
        int nextPage = getNextPageId(frame, -1);
        while (nextPage != -1) {
            ICachedPage page = pinPage(nextPage);
            try {
                frame.setPage(page);
                if (frame.get(key, value)) {
                    return true;
                }
                nextPage = getNextPageId(frame, nextPage);
            } finally {
                unpinPage(page);
            }
        }

        // To preserve the old behavior
        value.set(null, 0, 0);
        return false;
    }

    @Override
    public int getPageSize() {
        return bufferCache.getPageSize();
    }

    @Override
    public int getFreeSpace() throws HyracksDataException {
        if (currentPage == null) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_WRITE_AFTER_FLUSH_ATTEMPT);
        }
        ITreeIndexMetadataFrame frame = createMetadataFrame();
        frame.setPage(currentPage);
        return frame.getSpace() - frame.getKeyValueStorageOverhead();
    }

    private int getNextPageId(ITreeIndexMetadataFrame frame, int previousPageIdx) throws HyracksDataException {
        if (metadataPages.isEmpty()) {
            // Read-only (immutable)
            return previousPageIdx == -1 ? getMetadataPageId() : frame.getNextMetadataPage();
        }

        // Write (still mutable)
        int nextPageIdx = previousPageIdx + 1;
        return nextPageIdx < metadataPages.size() ? nextPageIdx : -1;
    }

    private ICachedPage pinPage(int pageId) throws HyracksDataException {
        if (!metadataPages.isEmpty()) {
            return metadataPages.get(pageId);
        }

        return bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId));
    }

    private void unpinPage(ICachedPage page) throws HyracksDataException {
        if (metadataPages.isEmpty()) {
            bufferCache.unpin(page);
        }
    }

    private void confiscateNext(ITreeIndexMetadataFrame metaFrame) throws HyracksDataException {
        ICachedPage metaNode = bufferCache.confiscatePage(BufferCache.INVALID_DPID);
        try {
            metaFrame.setPage(metaNode);
            metaFrame.init();
        } finally {
            metadataPages.add(metaNode);
            currentPage = metaNode;
        }
    }

    private void persist(IPageWriteFailureCallback failureCallback) throws HyracksDataException {
        IFIFOPageWriter pageWriter = bufferCache.createFIFOWriter(NoOpPageWriteCallback.INSTANCE, failureCallback,
                DefaultBufferCacheWriteContext.INSTANCE);
        ITreeIndexMetadataFrame metaFrame = frameFactory.createFrame();
        // Last page will have nextPage as -1
        int nextPage = -1;
        int pageId = getMaxPageId(metaFrame) + 1;
        final ICompressedPageWriter compressedPageWriter = bufferCache.getCompressedPageWriter(fileId);

        // Write pages in reverse order (first confiscated page will be the last one to be written)
        for (int i = metadataPages.size() - 1; i >= 0; i--) {
            ICachedPage page = metadataPages.get(i);
            metaFrame.setPage(page);
            metaFrame.setNextMetadataPage(nextPage);
            // The validity bit matters in the last written page only. No harm for setting this flag for all pages.
            metaFrame.setValid(true);

            page.setDiskPageId(BufferedFileHandle.getDiskPageId(fileId, pageId));
            compressedPageWriter.prepareWrite(page);
            pageWriter.write(page);
            nextPage = pageId++;
        }
        compressedPageWriter.endWriting();
    }
}
