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
package org.apache.hyracks.storage.common.file;

import static org.apache.hyracks.storage.common.buffercache.BufferCache.DEBUG;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.common.buffercache.AbstractBufferedFileIOManager;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheWriteContext;
import org.apache.hyracks.storage.common.compression.file.CompressedFileReference;
import org.apache.hyracks.storage.common.compression.file.ICompressedPageWriter;
import org.apache.hyracks.storage.common.compression.file.NoOpLAFWriter;
import org.apache.hyracks.util.IThreadStats;

public class BufferedFileHandle extends AbstractBufferedFileIOManager {
    private static final ByteBuffer ZERO_HEADER_TAIL = ByteBuffer.allocate(IBufferCache.RESERVED_HEADER_BYTES * 1024);

    private final int fileId;
    private final AtomicInteger refCount;

    protected BufferedFileHandle(int fileId, BufferCache bufferCache, IIOManager ioManager,
            BlockingQueue<BufferCacheHeaderHelper> headerPageCache, IPageReplacementStrategy pageReplacementStrategy) {
        super(bufferCache, ioManager, headerPageCache, pageReplacementStrategy);
        this.fileId = fileId;
        refCount = new AtomicInteger();
    }

    public int getFileId() {
        return fileId;
    }

    public int incReferenceCount() {
        return refCount.incrementAndGet();
    }

    public int decReferenceCount() {
        return refCount.decrementAndGet();
    }

    public int getReferenceCount() {
        return refCount.get();
    }

    public long getDiskPageId(int pageId) {
        return getDiskPageId(fileId, pageId);
    }

    @Override
    public void read(CachedPage cPage, IBufferCacheReadContext context, IThreadStats threadStats)
            throws HyracksDataException {
        final BufferCacheHeaderHelper header = checkoutHeaderHelper();
        try {
            setPageInfo(cPage);
            IFileHandle handle = getFileHandle();
            int pageSize = bufferCache.getPageSizeWithHeader();
            long bytesRead = header.readFromFile(ioManager, handle, getFirstPageOffset(cPage), pageSize);

            if (!verifyBytesRead(bufferCache.getPageSizeWithHeader(), bytesRead)) {
                return;
            }

            final ByteBuffer buf = context.processHeader(ioManager, this, header, cPage, threadStats);
            cPage.getBuffer().put(buf);
        } finally {
            returnHeaderHelper(header);
        }

        readExtraPages(cPage);
    }

    private void setPageInfo(CachedPage cPage) {
        cPage.setCompressedPageOffset(getFirstPageOffset(cPage));
        cPage.setCompressedPageSize(bufferCache.getPageSize());
    }

    private void readExtraPages(CachedPage cPage) throws HyracksDataException {
        final int totalPages = cPage.getFrameSizeMultiplier();
        if (totalPages > 1) {
            pageReplacementStrategy.fixupCapacityOnLargeRead(cPage);
            cPage.getBuffer().position(bufferCache.getPageSize());
            cPage.getBuffer().limit(totalPages * bufferCache.getPageSize());
            readToBuffer(cPage.getBuffer(), getExtraPageOffset(cPage));
        }
    }

    @Override
    protected void write(CachedPage cPage, BufferCacheHeaderHelper header, int totalPages, int extraBlockPageId,
            IBufferCacheWriteContext context) throws HyracksDataException {
        final ByteBuffer buf = cPage.getBuffer();
        final boolean contiguousLargePages = getPageId(cPage.getDiskPageId()) + 1 == extraBlockPageId;
        IFileHandle handle = getFileHandle();
        long bytesWritten;
        long offset;
        try {
            buf.limit(contiguousLargePages ? bufferCache.getPageSize() * totalPages : bufferCache.getPageSize());
            buf.position(0);
            ByteBuffer[] buffers = header.prepareWrite(cPage);
            offset = getFirstPageOffset(cPage);
            bytesWritten = context.write(ioManager, handle, offset, buffers);
        } finally {
            returnHeaderHelper(header);
        }

        long extraPagesEnd = offset + bytesWritten;
        if (totalPages > 1 && !contiguousLargePages) {
            buf.limit(totalPages * bufferCache.getPageSize());
            long extraOffset = getExtraPageOffset(cPage);
            long extraBytes = writeExtraToFile(buf, extraOffset);
            bytesWritten += extraBytes;
            extraPagesEnd = extraOffset + extraBytes;
        }

        if (totalPages > 1) {
            bytesWritten += writeLargePageHeaderTail(totalPages, extraPagesEnd);
        }

        // A page occupies totalPages slots of getPageSizeWithHeader() each, and after the tail is written it
        // fills every one of them.
        final long expectedWritten = (long) bufferCache.getPageSizeWithHeader() * totalPages;
        verifyBytesWritten(expectedWritten, bytesWritten);

        cPage.setCompressedPageOffset(offset);
        cPage.setCompressedPageSize((int) bytesWritten);
    }

    /**
     * Writes the bytes a large page leaves unwritten at its tail.
     * <p>
     * A page is addressed as {@code pageId * (pageSize + RESERVED_HEADER_BYTES)}, so a page spanning N slots
     * owns {@code N * (pageSize + RESERVED_HEADER_BYTES)} bytes, but only one {@code RESERVED_HEADER_BYTES}
     * header is written for the whole page: extra pages carry no header of their own. That leaves
     * {@code RESERVED_HEADER_BYTES * (N - 1)} bytes at the end of the page unwritten. Reads never look there,
     * so on a local file it can be left as a hole, but a file whose writes are mirrored to an append-only cloud
     * stream cannot skip forward: the stream would fall behind the page offsets by that much and every
     * subsequent write would be misaligned. Writing the tail out keeps the two byte-identical, and zeros are
     * what a local hole reads back as anyway.
     *
     * @param totalPages number of slots the page spans
     * @param tailOffset first unwritten byte, i.e. the end of the page's payload
     * @return the number of bytes written
     */
    private long writeLargePageHeaderTail(int totalPages, long tailOffset) throws HyracksDataException {
        long remaining = (long) IBufferCache.RESERVED_HEADER_BYTES * (totalPages - 1);
        long written = 0;
        final ByteBuffer zeros = ZERO_HEADER_TAIL.duplicate();
        while (remaining > 0) {
            int chunk = (int) Math.min(remaining, zeros.capacity());
            zeros.position(0);
            zeros.limit(chunk);
            written += writeExtraToFile(zeros, tailOffset + written);
            remaining -= chunk;
        }
        return written;
    }

    @Override
    public long getStartPageOffset(int pageId) throws HyracksDataException {
        return (long) pageId * bufferCache.getPageSizeWithHeader();
    }

    @Override
    public int getNumberOfPages() throws HyracksDataException {
        if (DEBUG) {
            assert getFileSize() % bufferCache.getPageSizeWithHeader() == 0;
        }
        return (int) (getFileSize() / bufferCache.getPageSizeWithHeader());
    }

    @Override
    public ICompressedPageWriter getCompressedPageWriter() {
        return NoOpLAFWriter.INSTACNE;
    }

    @Override
    public long getPagesTotalSize(int startPageId, int numberOfPages) throws HyracksDataException {
        // This could be an overestimate as we cannot determine for sure as extra pages do not have a header
        return (long) numberOfPages * bufferCache.getPageSizeWithHeader();
    }

    @Override
    protected long getFirstPageOffset(CachedPage cPage) {
        return getPageOffset(getPageId(cPage.getDiskPageId()));
    }

    @Override
    protected long getExtraPageOffset(CachedPage cPage) {
        return getPageOffset(cPage.getExtraBlockPageId());
    }

    public static long getDiskPageId(int fileId, int pageId) {
        return (((long) fileId) << 32) + pageId;
    }

    public static int getFileId(long dpid) {
        return (int) (dpid >> 32);
    }

    public static int getPageId(long dpid) {
        return (int) dpid;
    }

    public static BufferedFileHandle create(FileReference fileRef, int fileId, BufferCache bufferCache,
            IIOManager ioManager, BlockingQueue<BufferCacheHeaderHelper> headerPageCache,
            IPageReplacementStrategy pageReplacementStrategy) {
        if (fileRef.isCompressed()) {
            final CompressedFileReference cFileRef = (CompressedFileReference) fileRef;
            return new CompressedBufferedFileHandle(fileId, cFileRef.getLAFFileReference(), bufferCache, ioManager,
                    headerPageCache, pageReplacementStrategy);
        }
        return new BufferedFileHandle(fileId, bufferCache, ioManager, headerPageCache, pageReplacementStrategy);
    }

    private long getPageOffset(int pageId) {
        return (long) pageId * bufferCache.getPageSizeWithHeader();
    }
}
