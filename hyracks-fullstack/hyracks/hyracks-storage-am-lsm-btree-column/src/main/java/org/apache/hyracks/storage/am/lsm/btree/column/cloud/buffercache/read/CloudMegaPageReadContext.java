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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.cloud.buffercache.context.BufferCacheCloudReadContextUtil;
import org.apache.hyracks.cloud.buffercache.page.CloudCachedPage;
import org.apache.hyracks.cloud.io.ICloudIOManager;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.ColumnRanges;
import org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.disk.IPhysicalDrive;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.annotations.NotThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@NotThreadSafe
final class CloudMegaPageReadContext implements IBufferCacheReadContext {
    private static final Logger LOGGER = LogManager.getLogger();
    private final ColumnProjectorType operation;
    private final ColumnRanges columnRanges;
    private final IPhysicalDrive drive;

    private int numberOfContiguousPages;
    private int pageCounter;
    private InputStream gapStream;

    // For debugging
    private long streamOffset;
    private long remainingStreamBytes;

    CloudMegaPageReadContext(ColumnProjectorType operation, ColumnRanges columnRanges, IPhysicalDrive drive) {
        this.operation = operation;
        this.columnRanges = columnRanges;
        this.drive = drive;
    }

    public void prepare(int numberOfContiguousPages) throws HyracksDataException {
        close();
        this.numberOfContiguousPages = numberOfContiguousPages;
        pageCounter = 0;
    }

    @Override
    public void onPin(ICachedPage page) throws HyracksDataException {
        CloudCachedPage cachedPage = (CloudCachedPage) page;
        if (cachedPage.skipCloudStream()) {
            /*
             * This page is requested but the buffer cache has a valid copy in memory. Also, the page itself was
             * requested to be read from the cloud. Since this page is valid, no buffer cache read() will be performed.
             * As the buffer cache read() is also responsible for persisting the bytes read from the cloud, we can end
             * up writing the bytes of this page in the position of another page. Therefore, we should skip the bytes
             * for this particular page to avoid placing the bytes of this page into another page's position.
             */
            skipStreamIfOpened(cachedPage);
            pageCounter++;
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
            CachedPage cPage) throws HyracksDataException {
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
            readFromStream(ioManager, fileHandle, header, cPage, persist);
        } else {
            /*
             *  Here we can find a page that is planned for eviction, but it has not being evicted yet
             *  (i.e., empty = false). This could happen if the cursor is at a point the sweeper hasn't
             *  reached yet (i.e., cloudOnly = false). Thus, whatever is read from the disk is valid.
             */
            skipStreamIfOpened(cPage);
        }

        if (++pageCounter == numberOfContiguousPages) {
            close();
        }

        // Finally process the header
        return DEFAULT.processHeader(ioManager, fileHandle, header, cPage);
    }

    void close() throws HyracksDataException {
        if (gapStream != null) {
            if (remainingStreamBytes != 0) {
                LOGGER.warn("Closed cloud stream with nonzero bytes = {}", remainingStreamBytes);
            }

            try {
                gapStream.close();
                gapStream = null;
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
    }

    private void readFromStream(IOManager ioManager, BufferedFileHandle fileHandle, BufferCacheHeaderHelper header,
            CachedPage cPage, boolean persist) throws HyracksDataException {
        InputStream stream = getOrCreateStream(ioManager, fileHandle, cPage);
        ByteBuffer buffer = header.getBuffer();
        buffer.position(0);

        try {
            while (buffer.remaining() > 0) {
                int length = stream.read(buffer.array(), buffer.position(), buffer.remaining());
                if (length < 0) {
                    throw new IllegalStateException("Stream should not be empty!");
                }
                buffer.position(buffer.position() + length);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }

        // Flip the buffer after reading to restore the correct position
        buffer.flip();

        if (persist) {
            long offset = cPage.getCompressedPageOffset();
            ICloudIOManager cloudIOManager = (ICloudIOManager) ioManager;
            BufferCacheCloudReadContextUtil.persist(cloudIOManager, fileHandle.getFileHandle(), buffer, offset);
        }

        streamOffset += cPage.getCompressedPageSize();
        remainingStreamBytes -= cPage.getCompressedPageSize();
    }

    private InputStream getOrCreateStream(IOManager ioManager, BufferedFileHandle fileHandle, CachedPage cPage)
            throws HyracksDataException {
        if (gapStream != null) {
            return gapStream;
        }

        int requiredNumOfPages = numberOfContiguousPages - pageCounter;
        long offset = cPage.getCompressedPageOffset();
        int pageId = BufferedFileHandle.getPageId(cPage.getDiskPageId());
        long length = fileHandle.getPagesTotalSize(pageId, requiredNumOfPages);
        remainingStreamBytes = length;
        streamOffset = offset;
        LOGGER.info(
                "Cloud stream read for pageId={} starting from pageCounter={} out of "
                        + "numberOfContiguousPages={} (streamOffset = {}, remainingStreamBytes = {})",
                pageId, pageCounter, numberOfContiguousPages, streamOffset, remainingStreamBytes);

        ICloudIOManager cloudIOManager = (ICloudIOManager) ioManager;
        gapStream = cloudIOManager.cloudRead(fileHandle.getFileHandle(), offset, length);

        return gapStream;
    }

    private void skipStreamIfOpened(CachedPage cPage) throws HyracksDataException {
        if (gapStream == null) {
            return;
        }

        try {
            long remaining = cPage.getCompressedPageSize();
            while (remaining > 0) {
                remaining -= gapStream.skip(remaining);
            }
            streamOffset += cPage.getCompressedPageSize();
            remainingStreamBytes -= cPage.getCompressedPageSize();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

}
