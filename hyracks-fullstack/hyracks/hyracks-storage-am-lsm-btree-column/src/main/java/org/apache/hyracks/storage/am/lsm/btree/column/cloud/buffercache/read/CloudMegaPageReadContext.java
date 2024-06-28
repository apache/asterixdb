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
package org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.read;

import static org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType.MERGE;
import static org.apache.hyracks.storage.common.buffercache.context.read.DefaultBufferCacheReadContextProvider.DEFAULT;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.cloud.buffercache.context.BufferCacheCloudReadContextUtil;
import org.apache.hyracks.cloud.buffercache.page.CloudCachedPage;
import org.apache.hyracks.cloud.io.ICloudIOManager;
import org.apache.hyracks.cloud.io.stream.CloudInputStream;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.ColumnRanges;
import org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.disk.IPhysicalDrive;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.IThreadStats;
import org.apache.hyracks.util.annotations.NotThreadSafe;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@NotThreadSafe
public final class CloudMegaPageReadContext implements IBufferCacheReadContext {
    private static final Logger LOGGER = LogManager.getLogger();
    static final BitSet ALL_PAGES = new BitSet();
    private final ColumnProjectorType operation;
    private final ColumnRanges columnRanges;
    private final IPhysicalDrive drive;
    private final List<ICachedPage> pinnedPages;

    private int numberOfContiguousPages;
    private int pageCounter;
    private CloudInputStream gapStream;

    // for debugging
    int pageZeroId;

    CloudMegaPageReadContext(ColumnProjectorType operation, ColumnRanges columnRanges, IPhysicalDrive drive) {
        this.operation = operation;
        this.columnRanges = columnRanges;
        this.drive = drive;
        pinnedPages = new ArrayList<>();
    }

    void pin(IBufferCache bufferCache, int fileId, int pageZeroId, int start, int numberOfPages, BitSet requestedPages)
            throws HyracksDataException {
        closeStream();
        this.numberOfContiguousPages = numberOfPages;
        pageCounter = 0;
        this.pageZeroId = pageZeroId;
        doPin(bufferCache, fileId, pageZeroId, start, numberOfPages, requestedPages);
    }

    @Override
    public void onPin(ICachedPage page) throws HyracksDataException {
        CloudCachedPage cachedPage = (CloudCachedPage) page;
        if (cachedPage.skipCloudStream()) {
            /*
             * This page is requested but the buffer cache has a valid copy in memory. Also, the page itself was
             * gapStream requested to be read from the cloud. Since this page is valid, no buffer cache read() will be
             * performed. As the buffer cache read() is also responsible for persisting the bytes read from the cloud,
             * we can end up writing the bytes of this page in the position of another page. Therefore, we should skip
             * the bytes for this particular page to avoid placing the bytes of this page into another page's position.
             */
            skipStreamIfOpened(cachedPage);
        }
    }

    @Override
    public void onUnpin(ICachedPage page) {
        // NoOp
    }

    @Override
    public boolean isNewPage() {
        return false;
    }

    @Override
    public boolean incrementStats() {
        return true;
    }

    @Override
    public ByteBuffer processHeader(IOManager ioManager, BufferedFileHandle fileHandle, BufferCacheHeaderHelper header,
            CachedPage cPage, IThreadStats threadStats) throws HyracksDataException {
        boolean empty = BufferCacheCloudReadContextUtil.isEmpty(header);
        int pageId = BufferedFileHandle.getPageId(cPage.getDiskPageId());
        boolean cloudOnly = columnRanges.isCloudOnly(pageId);
        if (empty || cloudOnly) {
            boolean evictable = columnRanges.isEvictable(pageId);
            /*
             * Persist iff the following conditions are satisfied:
             * - The page is empty
             * - The page is not being evicted (cloudOnly)
             * - The page is not planned for eviction (evictable)
             * - The operation is not a merge operation (the component will be deleted anyway)
             * - The disk is not pressured
             *
             * Note: 'empty' can be false while 'cloudOnly is true'. We cannot read from disk as the page can be
             * evicted at any moment. In other words, the sweeper told us that it is going to evict this page; hence
             * 'cloudOnly' is true.
             */
            boolean persist = empty && !cloudOnly && !evictable && operation != MERGE && drive.isUnpressured();
            readFromStream(ioManager, fileHandle, header, cPage, persist, threadStats);
        } else {
            /*
             *  Here we can find a page that is planned for eviction, but it has not being evicted yet
             *  (i.e., empty = false). This could happen if the cursor is at a point the sweeper hasn't
             *  reached yet (i.e., cloudOnly = false). Thus, whatever is read from the disk is valid.
             */
            skipStreamIfOpened(cPage);
        }

        // Finally process the header
        return DEFAULT.processHeader(ioManager, fileHandle, header, cPage, threadStats);
    }

    void unpinAll(IBufferCache bufferCache) throws HyracksDataException {
        for (int i = 0; i < pinnedPages.size(); i++) {
            bufferCache.unpin(pinnedPages.get(i), this);
        }
        pinnedPages.clear();
    }

    void closeStream() {
        if (gapStream != null) {
            gapStream.close();
            gapStream = null;
        }
    }

    private void readFromStream(IOManager ioManager, BufferedFileHandle fileHandle, BufferCacheHeaderHelper header,
            CachedPage cPage, boolean persist, IThreadStats threadStats) throws HyracksDataException {
        CloudInputStream stream = getOrCreateStream(ioManager, fileHandle, cPage, threadStats);
        ByteBuffer buffer = header.getBuffer();
        buffer.position(0);

        /*
         * The 'gapStream' could point to an offset of an unwanted page due to range merging. For example, if
         * 'gapStream' is currently at the offset of pageId = 5 and the cPage is for pageId = 7, then, the stream
         * must be advanced to the cPage's offset (i.e., offset of pageId = 7) -- skipping pages 5 and 6.
         */
        gapStream.skipTo(cPage.getCompressedPageOffset());

        // Get the page's data from the cloud
        doStreamRead(stream, buffer, threadStats);

        // Flip the buffer after reading to restore the correct position
        buffer.flip();

        if (persist) {
            long offset = cPage.getCompressedPageOffset();
            ICloudIOManager cloudIOManager = (ICloudIOManager) ioManager;
            BufferCacheCloudReadContextUtil.persist(cloudIOManager, fileHandle.getFileHandle(), buffer, offset);
            threadStats.cloudPagePersist();
        }
    }

    private CloudInputStream getOrCreateStream(IOManager ioManager, BufferedFileHandle fileHandle, CachedPage cPage,
            IThreadStats threadStats) throws HyracksDataException {
        if (gapStream != null) {
            return gapStream;
        }

        int requiredNumOfPages = numberOfContiguousPages - pageCounter;
        long offset = cPage.getCompressedPageOffset();
        int pageId = BufferedFileHandle.getPageId(cPage.getDiskPageId());
        long length = fileHandle.getPagesTotalSize(pageId, requiredNumOfPages);
        ICloudIOManager cloudIOManager = (ICloudIOManager) ioManager;
        gapStream = cloudIOManager.cloudRead(fileHandle.getFileHandle(), offset, length);

        threadStats.cloudReadRequest();

        LOGGER.info(
                "Cloud stream read for pageId={} starting from pageCounter={} out of "
                        + "numberOfContiguousPages={}. pageZeroId={} stream: {}",
                pageId, pageCounter, numberOfContiguousPages, pageZeroId, gapStream);

        return gapStream;
    }

    private void doStreamRead(CloudInputStream stream, ByteBuffer buffer, IThreadStats threadStats)
            throws HyracksDataException {
        int length = buffer.remaining();
        try {
            stream.read(buffer);
            threadStats.cloudPageRead();
        } catch (Throwable th) {
            LOGGER.log(getLevel(th), "Failed to READ {} bytes from stream {}", length, gapStream, th);
            throw HyracksDataException.create(th);
        }
    }

    private void skipStreamIfOpened(CachedPage cPage) throws HyracksDataException {
        if (gapStream == null) {
            return;
        }

        // Ensure the stream starts from the page's offset and also skip the page's content
        long newOffset = cPage.getCompressedPageOffset() + cPage.getCompressedPageSize();
        try {
            gapStream.skipTo(newOffset);
        } catch (Throwable th) {
            LOGGER.log(getLevel(th), "Failed to SKIP to new offset {} from stream {}", newOffset, gapStream, th);
            throw HyracksDataException.create(th);
        }
    }

    private void doPin(IBufferCache bufferCache, int fileId, int pageZeroId, int start, int numberOfPages,
            BitSet requestedPages) throws HyracksDataException {
        for (int i = start; i < start + numberOfPages; i++) {
            try {
                if (requestedPages == ALL_PAGES || requestedPages.get(i)) {
                    int pageId = pageZeroId + i;
                    long dpid = BufferedFileHandle.getDiskPageId(fileId, pageId);
                    pinnedPages.add(bufferCache.pin(dpid, this));
                }
                pageCounter++;
            } catch (Throwable th) {
                LOGGER.log(getLevel(th),
                        "Error while pinning page number {} with number of pages {}. stream: {}, columnRanges:\n {}", i,
                        numberOfPages, gapStream, columnRanges, th);
                throw th;
            }
        }
    }

    private static Level getLevel(Throwable th) {
        return ExceptionUtils.causedByInterrupt(th) ? Level.DEBUG : Level.WARN;
    }
}
