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
package org.apache.hyracks.cloud.buffercache.context;

import static org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper.EXTRA_BLOCK_PAGE_ID_OFF;
import static org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper.FRAME_MULTIPLIER_OFF;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.cloud.io.ICloudIOManager;
import org.apache.hyracks.cloud.io.stream.CloudInputStream;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.disk.IPhysicalDrive;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.IThreadStats;
import org.apache.hyracks.util.annotations.ThreadSafe;

/**
 * Default context for {@link BufferCache#pin(long, IBufferCacheReadContext)}
 * and {@link IBufferCache#unpin(ICachedPage, IBufferCacheReadContext)} in a cloud deployment.
 * <p>
 * The default behavior of this context is persisting the pages read from cloud if {@link IPhysicalDrive}
 * reports that the local drive(s) are not pressured.
 */
@ThreadSafe
public class DefaultCloudReadContext implements IBufferCacheReadContext {
    private final IPhysicalDrive drive;
    private final long readAheadSize;

    /**
     * @param readAheadSize how many bytes to read from the cloud storage in one request for a page that is a hole in
     *                      the local file: the page and as many of the pages right after it in the file as fit, which
     *                      are all saved locally if the drive has space. 0 reads one page at a time.
     */
    public DefaultCloudReadContext(IPhysicalDrive drive, long readAheadSize) {
        this.drive = drive;
        this.readAheadSize = readAheadSize;
    }

    @Override
    public void onPin(ICachedPage page) {
        // NoOp
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
        boolean persist = drive.hasSpace();
        return readAndPersistPage(ioManager, fileHandle, header, cPage, threadStats, persist,
                persist ? readAheadSize : 0L);
    }

    public static ByteBuffer readAndPersistPage(IOManager ioManager, BufferedFileHandle fileHandle,
            BufferCacheHeaderHelper header, CachedPage cPage, IThreadStats threadStats, boolean persist)
            throws HyracksDataException {
        return readAndPersistPage(ioManager, fileHandle, header, cPage, threadStats, persist, 0L);
    }

    private static ByteBuffer readAndPersistPage(IOManager ioManager, BufferedFileHandle fileHandle,
            BufferCacheHeaderHelper header, CachedPage cPage, IThreadStats threadStats, boolean persist,
            long readAheadSize) throws HyracksDataException {
        ByteBuffer headerBuf =
                readAndPersistIfEmpty(ioManager, fileHandle, header, cPage, threadStats, persist, readAheadSize);

        cPage.setFrameSizeMultiplier(headerBuf.getInt(FRAME_MULTIPLIER_OFF));
        cPage.setExtraBlockPageId(headerBuf.getInt(EXTRA_BLOCK_PAGE_ID_OFF));
        headerBuf.position(BufferCache.RESERVED_HEADER_BYTES);
        return headerBuf;
    }

    /**
     * Note: {@link BufferCache} guarantees that no two reads on the same page can happen at the same time.
     * This means persisting the page on disk here is guaranteed to be done by a single thread.
     *
     * @param ioManager     ioManager (guaranteed to be {@link ICloudIOManager}
     * @param fileHandle    file
     * @param header        header buffer provider
     * @param cPage         cached page
     * @param readAheadSize how many bytes to read in one request with the page, the pages after it included
     * @return header buffer
     */
    private static ByteBuffer readAndPersistIfEmpty(IOManager ioManager, BufferedFileHandle fileHandle,
            BufferCacheHeaderHelper header, CachedPage cPage, IThreadStats threadStats, boolean persist,
            long readAheadSize) throws HyracksDataException {
        ByteBuffer headerBuf = header.getBuffer();
        boolean empty = BufferCacheCloudReadContextUtil.isEmpty(header);
        CloudCacheStats.pageRead(!empty, headerBuf.limit());
        if (empty) {
            // header indicates the page is empty
            // reset the buffer position to 0. Limit should be already set before the call of processHeader
            headerBuf.position(0);
            long offset = cPage.getCompressedPageOffset();
            ICloudIOManager cloudIOManager = (ICloudIOManager) ioManager;
            int pageId = BufferedFileHandle.getPageId(cPage.getDiskPageId());
            long end = offset + headerBuf.limit();
            long readAheadEnd =
                    readAheadSize > 0 ? getReadAheadEnd(fileHandle, pageId, offset, end, readAheadSize) : end;
            CloudInputStream stream = null;
            try {
                if (readAheadEnd > end) {
                    stream = cloudIOManager.cloudRead(fileHandle.getFileHandle(), offset, readAheadEnd - offset);
                    stream.read(headerBuf);
                } else {
                    // Read pageZero from the cloud
                    cloudIOManager.cloudRead(fileHandle.getFileHandle(), offset, headerBuf);
                }
                // accounting pageZero for cloud read
                threadStats.cloudPageRead();
                headerBuf.flip();

                // A large page is persisted by readHoleExtraPages, once its extra pages are read, so that its
                // header is written after them
                if (persist && headerBuf.getInt(FRAME_MULTIPLIER_OFF) == 1) {
                    BufferCacheCloudReadContextUtil.persist(cloudIOManager, fileHandle, headerBuf, offset);
                    threadStats.cloudPagePersist();
                }
                if (stream != null) {
                    persistPagesAhead(cloudIOManager, fileHandle, stream, pageId + 1, headerBuf.capacity(),
                            threadStats);
                }
            } finally {
                if (stream != null) {
                    stream.close();
                }
            }
        }

        return headerBuf;
    }

    /**
     * @return the end of the run of pages stored right after the page at {@code [offset, end)} that fits, with that
     *         page, in {@code readAheadSize} bytes; {@code end} if no page does. A compressed file's pages are where
     *         its look-aside file says, which is not always in page id order.
     */
    private static long getReadAheadEnd(BufferedFileHandle handle, int pageId, long offset, long end,
            long readAheadSize) throws HyracksDataException {
        int numberOfPages = handle.getNumberOfPages();
        for (int next = pageId + 1; next < numberOfPages; next++) {
            long size = handle.getPagesTotalSize(next, 1);
            if (handle.getStartPageOffset(next) != end || end + size - offset > readAheadSize) {
                break;
            }
            end += size;
        }
        return end;
    }

    /**
     * Saves the pages left in {@code stream}, which starts at the page with id {@code firstPageId}, as they are in
     * the cloud storage. A large page is left a hole: its header must not be written before all of its extra pages,
     * which may be elsewhere in the file. Rewriting a page that is already saved writes the same bytes again.
     */
    private static void persistPagesAhead(ICloudIOManager cloudIOManager, BufferedFileHandle handle,
            CloudInputStream stream, int firstPageId, int pageCapacity, IThreadStats threadStats)
            throws HyracksDataException {
        // a page is stored in at most a page and its header, which fits the header buffer
        ByteBuffer page = ByteBuffer.allocate(pageCapacity);
        for (int pageId = firstPageId; stream.getRemaining() > 0; pageId++) {
            int size = Math.toIntExact(handle.getPagesTotalSize(pageId, 1));
            page.clear();
            page.limit(size);
            stream.read(page);
            threadStats.cloudPageRead();
            page.flip();
            if (page.getInt(FRAME_MULTIPLIER_OFF) == 1) {
                BufferCacheCloudReadContextUtil.persist(cloudIOManager, handle, page,
                        handle.getStartPageOffset(pageId));
                threadStats.cloudPagePersist();
            }
        }
    }

    @Override
    public void readHoleExtraPages(IOManager ioManager, BufferedFileHandle fileHandle, ByteBuffer firstPage,
            long firstPageOffset, ByteBuffer extraPages, long extraPagesOffset, IThreadStats threadStats)
            throws HyracksDataException {
        ICloudIOManager cloudIOManager = (ICloudIOManager) ioManager;
        int start = extraPages.position();
        cloudIOManager.cloudRead(fileHandle.getFileHandle(), extraPagesOffset, extraPages);
        threadStats.cloudPageRead();

        if (drive.hasSpace()) {
            ByteBuffer read = extraPages.duplicate();
            read.position(start);
            fileHandle.doCacheWriteOnHandle(handle -> cloudIOManager.localWriter(handle, extraPagesOffset, read));
            // Writes the first page's header last, committing the whole page
            BufferCacheCloudReadContextUtil.persist(cloudIOManager, fileHandle, firstPage, firstPageOffset);
            threadStats.cloudPagePersist();
        }
    }
}
