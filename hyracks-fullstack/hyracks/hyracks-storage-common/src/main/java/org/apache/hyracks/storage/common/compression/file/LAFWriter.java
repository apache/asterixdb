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
package org.apache.hyracks.storage.common.compression.file;

import static org.apache.hyracks.storage.common.compression.file.CompressedFileManager.ENTRY_LENGTH;
import static org.apache.hyracks.storage.common.compression.file.CompressedFileManager.EOF;
import static org.apache.hyracks.storage.common.compression.file.CompressedFileManager.SIZE_ENTRY_OFFSET;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.ICachedPageInternal;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageWriter;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.PageWriteFailureCallback;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.annotations.NotThreadSafe;

/**
 * Look Aside File writer
 */
@NotThreadSafe
class LAFWriter implements ICompressedPageWriter {
    private final CompressedFileManager compressedFileManager;
    private final IBufferCache bufferCache;
    private final IFIFOPageWriter pageWriter;
    private final Map<Integer, LAFFrame> cachedFrames;
    private final Queue<LAFFrame> recycledFrames;
    private final int fileId;
    private final int maxNumOfEntries;
    private final PageWriteFailureCallback failureCallback;
    private LAFFrame currentFrame;
    private int currentPageId;
    private int maxPageId;

    private long lastOffset;
    private int totalNumOfPages;

    public LAFWriter(CompressedFileManager compressedFileManager, IBufferCache bufferCache) {
        this.compressedFileManager = compressedFileManager;
        this.bufferCache = bufferCache;
        cachedFrames = new HashMap<>();
        recycledFrames = new ArrayDeque<>();
        this.fileId = compressedFileManager.getFileId();
        failureCallback = new PageWriteFailureCallback();
        pageWriter = bufferCache.createFIFOWriter(NoOpPageWriteCallback.INSTANCE, failureCallback);
        maxNumOfEntries = bufferCache.getPageSize() / ENTRY_LENGTH;
        lastOffset = 0;
        totalNumOfPages = 0;
        maxPageId = -1;
        currentPageId = -1;
    }

    /* ************************************
     * ICompressedPageWriter methods
     * Called by non-BufferCache (Bulk-loader)
     * ************************************
     */

    @Override
    public void prepareWrite(ICachedPage cPage) throws HyracksDataException {
        final ICachedPageInternal internalPage = (ICachedPageInternal) cPage;
        final int entryPageId = getLAFEntryPageId(BufferedFileHandle.getPageId(internalPage.getDiskPageId()));

        if (!cachedFrames.containsKey(entryPageId)) {
            try {
                //Writing new page(s). Confiscate the page(s) from the buffer cache.
                prepareFrames(entryPageId, internalPage);
            } catch (HyracksDataException e) {
                abort();
                throw e;
            }
        }
    }

    private void prepareFrames(int entryPageId, ICachedPageInternal cPage) throws HyracksDataException {
        //Confiscate the first page
        confiscatePage(entryPageId);
        //check if extra pages spans to the next entry page
        for (int i = 0; i < cPage.getFrameSizeMultiplier() - 1; i++) {
            final int extraEntryPageId = getLAFEntryPageId(cPage.getExtraBlockPageId() + i);
            if (!cachedFrames.containsKey(extraEntryPageId)) {
                confiscatePage(extraEntryPageId);
            }
        }
    }

    private void confiscatePage(int pageId) throws HyracksDataException {
        //Writing new page. Confiscate the page from the buffer cache.
        final ICachedPage newPage = bufferCache.confiscatePage(BufferedFileHandle.getDiskPageId(fileId, pageId));
        cachedFrames.put(pageId, getLAFFrame(newPage));
        maxPageId = Math.max(maxPageId, pageId);
    }

    private LAFFrame getLAFFrame(ICachedPage cPage) {
        LAFFrame lafFrame = recycledFrames.poll();
        if (lafFrame == null) {
            lafFrame = new LAFFrame();
        }
        lafFrame.setCachedPage(cPage);
        return lafFrame;
    }

    @Override
    public void endWriting() throws HyracksDataException {
        if (failureCallback.hasFailed()) {
            //if write failed, return confiscated pages
            abort();
            throw HyracksDataException.create(failureCallback.getFailure());
        }
        final LAFFrame lastPage = cachedFrames.get(maxPageId);
        if (lastPage != null && !lastPage.isFull()) {
            /*
             * The last page may or may not be filled. In case it is not filled (i.e do not have
             * the max number of entries). Then, write an indicator after the last entry.
             * If it has been written (i.e lastPage = null), that means it has been filled.
             */
            lastPage.setEOF();
        }
        for (Entry<Integer, LAFFrame> entry : cachedFrames.entrySet()) {
            pageWriter.write(entry.getValue().cPage);
        }

        //Signal the compressedFileManager to change its state
        compressedFileManager.endWriting(totalNumOfPages);
    }

    @Override
    public void abort() {
        for (Entry<Integer, LAFFrame> frame : cachedFrames.entrySet()) {
            bufferCache.returnPage(frame.getValue().cPage);
        }
    }

    /* ************************************
     * Local methods:
     * Called by BufferCache writer
     * ************************************
     */

    public long writePageInfo(int pageId, long size) throws HyracksDataException {
        final LAFFrame frame = getPageBuffer(pageId);

        final long pageOffset = lastOffset;
        frame.writePageInfo(pageId, pageOffset, size);
        lastOffset += size;
        totalNumOfPages++;

        writeFullPage();
        return pageOffset;
    }

    private LAFFrame getPageBuffer(int compressedPageId) {
        final int pageId = getLAFEntryPageId(compressedPageId);

        if (currentPageId == pageId) {
            return currentFrame;
        }

        final LAFFrame frame;
        //Check if the frame is cached
        frame = cachedFrames.get(pageId);
        if (frame == null) {
            //Trying to write unprepared page
            abort();
            throw new IllegalStateException("Unprepared compressed-write for page ID: " + pageId);
        }

        currentFrame = frame;
        currentPageId = pageId;
        return frame;
    }

    private void writeFullPage() throws HyracksDataException {
        if (currentFrame.isFull()) {
            //The LAF page is filled. We do not need to keep it.
            //Write it to the file and remove it from the cachedFrames map
            pageWriter.write(currentFrame.cPage);
            //Recycle the frame
            final LAFFrame frame = cachedFrames.remove(currentPageId);
            frame.setCachedPage(null);
            recycledFrames.add(frame);
            currentFrame = null;
            currentPageId = -1;
        }
    }

    private int getLAFEntryPageId(int compressedPageId) {
        return compressedPageId * ENTRY_LENGTH / bufferCache.getPageSize();
    }

    private class LAFFrame {
        private ICachedPage cPage;
        private int numOfEntries;
        private int maxEntryOffset;

        public void setCachedPage(ICachedPage cPage) {
            this.cPage = cPage;
            numOfEntries = 0;
            maxEntryOffset = -1;
        }

        public void writePageInfo(int compressedPageId, long offset, long size) {
            final int entryOffset = compressedPageId * ENTRY_LENGTH % bufferCache.getPageSize();
            //Put page offset
            cPage.getBuffer().putLong(entryOffset, offset);
            //Put page size
            cPage.getBuffer().putLong(entryOffset + SIZE_ENTRY_OFFSET, size);
            //Keep the max entry offset to set EOF (if needed)
            maxEntryOffset = Math.max(maxEntryOffset, entryOffset);
            numOfEntries++;
        }

        public void setEOF() {
            cPage.getBuffer().putLong(maxEntryOffset + ENTRY_LENGTH, EOF);
        }

        public boolean isFull() {
            return numOfEntries == maxNumOfEntries;
        }
    }

}
