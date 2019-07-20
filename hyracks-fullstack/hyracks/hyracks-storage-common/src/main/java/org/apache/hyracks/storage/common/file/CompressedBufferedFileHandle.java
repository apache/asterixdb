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

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

import org.apache.hyracks.api.compression.ICompressorDecompressor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import org.apache.hyracks.storage.common.compression.file.CompressedFileManager;
import org.apache.hyracks.storage.common.compression.file.CompressedFileReference;
import org.apache.hyracks.storage.common.compression.file.ICompressedPageWriter;

public class CompressedBufferedFileHandle extends BufferedFileHandle {
    private final FileReference lafFileRef;
    private volatile CompressedFileManager compressedFileManager;

    protected CompressedBufferedFileHandle(int fileId, FileReference lafFileRef, BufferCache bufferCache,
            IIOManager ioManager, BlockingQueue<BufferCacheHeaderHelper> headerPageCache,
            IPageReplacementStrategy pageReplacementStrategy) {
        super(fileId, bufferCache, ioManager, headerPageCache, pageReplacementStrategy);
        this.lafFileRef = lafFileRef;
    }

    @Override
    public void read(CachedPage cPage) throws HyracksDataException {
        final BufferCacheHeaderHelper header = checkoutHeaderHelper();
        try {
            compressedFileManager.setCompressedPageInfo(cPage);
            long bytesRead = readToBuffer(header.prepareRead(cPage.getCompressedPageSize()), getFirstPageOffset(cPage));

            if (!verifyBytesRead(cPage.getCompressedPageSize(), bytesRead)) {
                return;
            }
            final ByteBuffer cBuffer = header.processHeader(cPage);
            final ByteBuffer uBuffer = cPage.getBuffer();
            fixBufferPointers(uBuffer, 0);
            if (cPage.getCompressedPageSize() < bufferCache.getPageSizeWithHeader()) {
                uncompressToPageBuffer(cBuffer, uBuffer);
            } else {
                cPage.getBuffer().put(cBuffer);
            }

            final int totalPages = cPage.getFrameSizeMultiplier();
            if (totalPages > 1) {
                pageReplacementStrategy.fixupCapacityOnLargeRead(cPage);
                readExtraPages(cPage, cBuffer);
            }
        } finally {
            returnHeaderHelper(header);
        }
    }

    private void readExtraPages(CachedPage cPage, ByteBuffer cBuffer) throws HyracksDataException {
        final ByteBuffer uBuffer = cPage.getBuffer();

        final int totalPages = cPage.getFrameSizeMultiplier();
        for (int i = 1; i < totalPages; i++) {
            fixBufferPointers(uBuffer, i);
            compressedFileManager.setExtraCompressedPageInfo(cPage, i - 1);
            if (cPage.getCompressedPageSize() < bufferCache.getPageSize()) {
                cBuffer.position(0);
                cBuffer.limit(cPage.getCompressedPageSize());
                readToBuffer(cBuffer, getExtraPageOffset(cPage));
                cBuffer.flip();
                uncompressToPageBuffer(cBuffer, cPage.getBuffer());
            } else {
                readToBuffer(uBuffer, getExtraPageOffset(cPage));
            }
        }
    }

    @Override
    protected void write(CachedPage cPage, BufferCacheHeaderHelper header, int totalPages, int extraBlockPageId)
            throws HyracksDataException {
        try {
            final ByteBuffer cBuffer = header.prepareWrite(cPage, getRequiredBufferSize());
            final ByteBuffer uBuffer = cPage.getBuffer();
            final long pageId = cPage.getDiskPageId();

            final long bytesWritten;
            final long expectedBytesWritten;

            fixBufferPointers(uBuffer, 0);
            if (compressToWriteBuffer(uBuffer, cBuffer) < bufferCache.getPageSize()) {
                cBuffer.position(0);
                final long offset = compressedFileManager.writePageInfo(pageId, cBuffer.remaining());
                expectedBytesWritten = cBuffer.limit();
                bytesWritten = writeToFile(cBuffer, offset);
            } else {
                //Compression did not gain any savings
                final ByteBuffer[] buffers = header.prepareWrite(cPage);
                final long offset = compressedFileManager.writePageInfo(pageId, bufferCache.getPageSizeWithHeader());
                expectedBytesWritten = buffers[0].limit() + (long) buffers[1].limit();
                bytesWritten = writeToFile(buffers, offset);
            }

            verifyBytesWritten(expectedBytesWritten, bytesWritten);

            //Write extra pages
            if (totalPages > 1) {
                writeExtraCompressedPages(cPage, cBuffer, totalPages, extraBlockPageId);
            }

        } finally {
            returnHeaderHelper(header);
        }
    }

    private void writeExtraCompressedPages(CachedPage cPage, ByteBuffer cBuffer, int totalPages, int extraBlockPageId)
            throws HyracksDataException {
        final ByteBuffer uBuffer = cPage.getBuffer();
        long expectedBytesWritten = 0;
        long bytesWritten = 0;
        for (int i = 1; i < totalPages; i++) {
            fixBufferPointers(uBuffer, i);
            cBuffer.position(0);

            final ByteBuffer writeBuffer;
            if (compressToWriteBuffer(uBuffer, cBuffer) < bufferCache.getPageSize()) {
                writeBuffer = cBuffer;
            } else {
                writeBuffer = uBuffer;
            }
            final int length = writeBuffer.remaining();
            final long offset = compressedFileManager.writeExtraPageInfo(extraBlockPageId, length, i - 1);
            expectedBytesWritten += length;
            bytesWritten += writeToFile(writeBuffer, offset);
        }

        verifyBytesWritten(expectedBytesWritten, bytesWritten);

    }

    @Override
    public void open(FileReference fileRef) throws HyracksDataException {
        final CompressedFileReference cFileRef = (CompressedFileReference) fileRef;
        compressedFileManager = new CompressedFileManager(bufferCache, cFileRef);
        compressedFileManager.open();
        super.open(fileRef);
    }

    /**
     * Decrement the reference counter for LAF file.
     * It is up to {@link BufferCache} to physically close the file.
     * see {@link BufferCache#deleteFile(FileReference)} and {@link BufferCache#purgeHandle(int)}
     */
    @Override
    public void close() throws HyracksDataException {
        if (hasBeenOpened()) {
            compressedFileManager.close();
        }
        super.close();
    }

    @Override
    public void purge() throws HyracksDataException {
        compressedFileManager.purge();
        super.purge();
    }

    @Override
    public void markAsDeleted() throws HyracksDataException {
        if (hasBeenOpened()) {
            compressedFileManager.delete();
            compressedFileManager = null;
        } else {
            bufferCache.deleteFile(lafFileRef);
        }
        super.markAsDeleted();
    }

    @Override
    public void force(boolean metadata) throws HyracksDataException {
        compressedFileManager.force(metadata);
        super.force(metadata);
    }

    @Override
    public int getNumberOfPages() {
        return compressedFileManager.getNumberOfPages();
    }

    @Override
    protected long getFirstPageOffset(CachedPage cPage) {
        return cPage.getCompressedPageOffset();
    }

    @Override
    protected long getExtraPageOffset(CachedPage cPage) {
        return getFirstPageOffset(cPage);
    }

    @Override
    public ICompressedPageWriter getCompressedPageWriter() {
        return compressedFileManager.getCompressedPageWriter();
    }

    /* ********************************
     * Compression methods
     * ********************************
     */

    private void fixBufferPointers(ByteBuffer uBuffer, int i) {
        //Fix the uncompressed buffer to point at the i^th extra page
        uBuffer.position(bufferCache.getPageSize() * i);
        //Similarly, fix the limit to a page-worth of data from the i^th page
        uBuffer.limit(uBuffer.position() + bufferCache.getPageSize());
    }

    private void uncompressToPageBuffer(ByteBuffer cBuffer, ByteBuffer uBuffer) throws HyracksDataException {
        final ICompressorDecompressor compDecomp = compressedFileManager.getCompressorDecompressor();
        compDecomp.uncompress(cBuffer, uBuffer);
        verifyUncompressionSize(bufferCache.getPageSize(), uBuffer.remaining());
    }

    private int compressToWriteBuffer(ByteBuffer uBuffer, ByteBuffer cBuffer) throws HyracksDataException {
        final ICompressorDecompressor compDecomp = compressedFileManager.getCompressorDecompressor();
        compDecomp.compress(uBuffer, cBuffer);
        return cBuffer.remaining();
    }

    private int getRequiredBufferSize() {
        final ICompressorDecompressor compDecomp = compressedFileManager.getCompressorDecompressor();
        return compDecomp.computeCompressedBufferSize(bufferCache.getPageSize());
    }

    private void verifyUncompressionSize(int expected, int actual) {
        if (expected != actual) {
            throwException("Uncompressed", expected, actual);
        }
    }

}
