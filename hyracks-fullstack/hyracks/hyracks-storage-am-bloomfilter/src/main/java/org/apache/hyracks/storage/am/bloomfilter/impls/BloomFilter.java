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

package org.apache.hyracks.storage.am.bloomfilter.impls;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageWriter;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.PageWriteFailureCallback;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class BloomFilter {

    private static final int METADATA_PAGE_ID = 0;
    private static final int NUM_PAGES_OFFSET = 0; // 0
    private static final int NUM_HASHES_USED_OFFSET = NUM_PAGES_OFFSET + 4; // 4
    private static final int NUM_ELEMENTS_OFFSET = NUM_HASHES_USED_OFFSET + 4; // 8
    private static final int NUM_BITS_OFFSET = NUM_ELEMENTS_OFFSET + 8; // 12
    private static final int VERSION_OFFSET = NUM_BITS_OFFSET + 8; // 20

    // we use cache line size as the block size (64 bytes)
    private static final int NUM_BITS_PER_BLOCK = 64 * 8;

    private static final int DEFAULT_BLOOM_FILTER_VERSION = 0;

    private static final int BLOCKED_BLOOM_FILTER_VERSION = 1;

    private final IBufferCache bufferCache;
    private final FileReference file;
    private final int[] keyFields;
    private int fileId = -1;
    private boolean isActivated = false;

    private int numPages;
    private int numHashes;
    private long numElements;
    private long numBits;
    // keep track of the version of the bloomfilter to be backward compatible
    private int version;
    private final int numBitsPerPage;
    private final int numBlocksPerPage;
    private ICachedPage[] pages;
    private int pinCount = 0;
    private boolean pagesPinned = false;
    private static final byte[] ZERO_BUFFER = new byte[131072]; // 128kb
    private static final long SEED = 0L;

    public BloomFilter(IBufferCache bufferCache, FileReference file, int[] keyFields) throws HyracksDataException {
        this.bufferCache = bufferCache;
        this.file = file;
        this.keyFields = keyFields;
        this.numBitsPerPage = bufferCache.getPageSize() * Byte.SIZE;
        this.numBlocksPerPage = this.numBitsPerPage / NUM_BITS_PER_BLOCK;
    }

    public int getFileId() {
        return fileId;
    }

    public FileReference getFileReference() {
        return file;
    }

    public synchronized void pinAllPages() throws HyracksDataException {
        if (pinCount == 0) {
            // first time pin
            if (pages == null) {
                pages = new ICachedPage[numPages];
            }
            for (int i = 0; i < numPages; i++) {
                pages[i] = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, i + 1), false);
            }
            pagesPinned = true;
        }
        pinCount++;
    }

    public synchronized void unpinAllPages() throws HyracksDataException {
        if (pinCount == 1) {
            for (int i = 0; i < numPages; i++) {
                bufferCache.unpin(pages[i]);
            }
            pagesPinned = false;
        }
        pinCount--;
    }

    public int getNumPages() throws HyracksDataException {
        if (!isActivated) {
            activate();
        }
        return numPages;
    }

    public long getNumElements() throws HyracksDataException {
        if (!isActivated) {
            throw HyracksDataException.create(ErrorCode.CANNOT_GET_NUMBER_OF_ELEMENT_FROM_INACTIVE_FILTER);
        }
        return numElements;
    }

    public boolean contains(ITupleReference tuple, long[] hashes) throws HyracksDataException {
        if (numPages == 0) {
            return false;
        }
        MurmurHash128Bit.hash3_x64_128(tuple, keyFields, SEED, hashes);
        if (version == BLOCKED_BLOOM_FILTER_VERSION) {
            return blockContains(hashes);
        } else {
            return legacyContains(hashes);
        }
    }

    private boolean blockContains(long[] hashes) throws HyracksDataException {
        // take first hash to compute block id
        long hash = Math.abs(hashes[0] % numBits);
        long blockId = hash / NUM_BITS_PER_BLOCK;
        int pageId = (int) (blockId / numBlocksPerPage);
        long groupStartIndex = (blockId % numBlocksPerPage) * NUM_BITS_PER_BLOCK;

        boolean unpinWhenExit = false;
        ICachedPage page = null;
        if (pagesPinned) {
            page = pages[pageId];
        } else {
            page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId + 1), false);
            unpinWhenExit = true;
        }
        ByteBuffer buffer = page.getBuffer();

        try {
            for (int i = 1; i < numHashes; ++i) {
                hash = Math.abs((hashes[0] + i * hashes[1]) % NUM_BITS_PER_BLOCK);
                int byteIndex = (int) ((hash + groupStartIndex) >> 3); // divide 8
                byte b = buffer.get(byteIndex);
                int bitIndex = (int) (hash & 0x07); // mod 8
                if (!((b & (1L << bitIndex)) != 0)) {
                    return false;
                }
            }
        } finally {
            if (unpinWhenExit) {
                bufferCache.unpin(page);
            }
        }
        return true;

    }

    // membership check for legacy bloom filters
    private boolean legacyContains(long[] hashes) throws HyracksDataException {
        for (int i = 0; i < numHashes; ++i) {
            long hash = Math.abs((hashes[0] + i * hashes[1]) % numBits);

            // we increment the page id by one, since the metadata page id of the filter is 0.
            ICachedPage page =
                    bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) (hash / numBitsPerPage) + 1), false);
            page.acquireReadLatch();
            try {
                ByteBuffer buffer = page.getBuffer();
                int byteIndex = (int) (hash % numBitsPerPage) >> 3; // divide by 8
                byte b = buffer.get(byteIndex);
                int bitIndex = (int) (hash % numBitsPerPage) & 0x07; // mod 8

                if (!((b & (1L << bitIndex)) != 0)) {
                    return false;
                }
            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }

        }
        return true;
    }

    public synchronized void create() throws HyracksDataException {
        if (isActivated) {
            throw HyracksDataException.create(ErrorCode.CANNOT_CREATE_ACTIVE_BLOOM_FILTER);
        }
        fileId = bufferCache.createFile(file);
    }

    public synchronized void activate() throws HyracksDataException {
        if (isActivated) {
            return;
        }
        if (fileId >= 0) {
            bufferCache.openFile(fileId);
        } else {
            fileId = bufferCache.openFile(file);
        }
        readBloomFilterMetaData();
        isActivated = true;
    }

    private void readBloomFilterMetaData() throws HyracksDataException {
        if (bufferCache.getNumPagesOfFile(fileId) == 0) {
            numPages = 0;
            numHashes = 0;
            numElements = 0;
            numBits = 0;
            version = DEFAULT_BLOOM_FILTER_VERSION;
            return;
        }
        ICachedPage metaPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, METADATA_PAGE_ID), false);
        metaPage.acquireReadLatch();
        try {
            numPages = metaPage.getBuffer().getInt(NUM_PAGES_OFFSET);
            numHashes = metaPage.getBuffer().getInt(NUM_HASHES_USED_OFFSET);
            numElements = metaPage.getBuffer().getLong(NUM_ELEMENTS_OFFSET);
            numBits = metaPage.getBuffer().getLong(NUM_BITS_OFFSET);
            version = metaPage.getBuffer().getInt(VERSION_OFFSET);
        } finally {
            metaPage.releaseReadLatch();
            bufferCache.unpin(metaPage);
        }
    }

    public synchronized void deactivate() throws HyracksDataException {
        if (!isActivated) {
            throw HyracksDataException.create(ErrorCode.CANNOT_DEACTIVATE_INACTIVE_BLOOM_FILTER);
        }
        if (pagesPinned) {
            throw HyracksDataException.create(ErrorCode.CANNOT_DEACTIVATE_PINNED_BLOOM_FILTER);
        }
        bufferCache.closeFile(fileId);
        isActivated = false;
    }

    public void purge() throws HyracksDataException {
        if (isActivated) {
            throw HyracksDataException.create(ErrorCode.CANNOT_PURGE_ACTIVE_BLOOM_FILTER);
        }
        bufferCache.purgeHandle(fileId);
        // after purging, the fileId has no mapping and no meaning
        fileId = -1;
    }

    public static long[] createHashArray() {
        return new long[2];
    }

    public synchronized void destroy() throws HyracksDataException {
        if (isActivated) {
            throw HyracksDataException.create(ErrorCode.CANNOT_DESTROY_ACTIVE_BLOOM_FILTER);
        }
        bufferCache.deleteFile(file);
    }

    public IIndexBulkLoader createBuilder(long numElements, int numHashes, int numBitsPerElement,
            IPageWriteCallback callback) throws HyracksDataException {
        return new BloomFilterBuilder(numElements, numHashes, numBitsPerElement, callback);
    }

    public class BloomFilterBuilder extends PageWriteFailureCallback implements IIndexBulkLoader {
        private final long[] hashes = BloomFilter.createHashArray();
        private final long estimatedNumElements;
        private final int numHashes;
        private final long numBits;
        private final int numPages;
        private long actualNumElements;
        private final IFIFOPageWriter pageWriter;
        private final ICachedPage[] pages;
        private ICachedPage metaDataPage = null;

        @SuppressWarnings("squid:S1181") // Catch Throwable Must return all confiscated pages
        public BloomFilterBuilder(long estimatedNumElemenets, int numHashes, int numBitsPerElement,
                IPageWriteCallback callback) throws HyracksDataException {
            if (!isActivated) {
                throw HyracksDataException.create(ErrorCode.CANNOT_CREATE_BLOOM_FILTER_BUILDER_FOR_INACTIVE_FILTER);
            }
            pageWriter = bufferCache.createFIFOWriter(callback, this);
            this.estimatedNumElements = estimatedNumElemenets;
            this.numHashes = numHashes;
            numBits = this.estimatedNumElements * numBitsPerElement;
            long tmp = (long) Math.ceil(numBits / (double) numBitsPerPage);
            if (tmp > Integer.MAX_VALUE) {
                throw HyracksDataException.create(ErrorCode.CANNOT_CREATE_BLOOM_FILTER_WITH_NUMBER_OF_PAGES, tmp);
            }
            numPages = (int) tmp;
            actualNumElements = 0;
            pages = new ICachedPage[numPages];
            int currentPageId = 1;
            try {
                while (currentPageId <= numPages) {
                    ICachedPage page =
                            bufferCache.confiscatePage(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
                    initPage(page.getBuffer().array());
                    pages[currentPageId - 1] = page;
                    ++currentPageId;
                }
            } catch (Throwable th) {
                // return confiscated pages
                for (int i = 0; i < currentPageId; i++) {
                    if (pages[i] != null) {
                        bufferCache.returnPage(pages[i]);
                    }
                }
                throw th;
            }
        }

        private void initPage(byte[] array) {
            int numRounds = array.length / ZERO_BUFFER.length;
            int leftOver = array.length % ZERO_BUFFER.length;
            int destPos = 0;
            for (int i = 0; i < numRounds; i++) {
                System.arraycopy(ZERO_BUFFER, 0, array, destPos, ZERO_BUFFER.length);
                destPos = (i + 1) * ZERO_BUFFER.length;
            }
            if (leftOver > 0) {
                System.arraycopy(ZERO_BUFFER, 0, array, destPos, leftOver);
            }
        }

        private void allocateAndInitMetaDataPage() throws HyracksDataException {
            if (metaDataPage == null) {
                metaDataPage = bufferCache.confiscatePage(BufferedFileHandle.getDiskPageId(fileId, METADATA_PAGE_ID));
            }
            metaDataPage.getBuffer().putInt(NUM_PAGES_OFFSET, numPages);
            metaDataPage.getBuffer().putInt(NUM_HASHES_USED_OFFSET, numHashes);
            metaDataPage.getBuffer().putLong(NUM_ELEMENTS_OFFSET, actualNumElements);
            metaDataPage.getBuffer().putLong(NUM_BITS_OFFSET, numBits);
            metaDataPage.getBuffer().putInt(VERSION_OFFSET, BLOCKED_BLOOM_FILTER_VERSION);
        }

        @Override
        public void add(ITupleReference tuple) throws HyracksDataException {
            if (numPages == 0) {
                throw HyracksDataException.create(ErrorCode.CANNOT_ADD_TUPLES_TO_DUMMY_BLOOM_FILTER);
            }
            actualNumElements++;
            MurmurHash128Bit.hash3_x64_128(tuple, keyFields, SEED, hashes);

            long hash = Math.abs(hashes[0] % numBits);
            long groupId = hash / NUM_BITS_PER_BLOCK;
            int pageId = (int) (groupId / numBlocksPerPage);
            long groupStartIndex = (groupId % numBlocksPerPage) * NUM_BITS_PER_BLOCK;

            ICachedPage page = pages[pageId];
            ByteBuffer buffer = page.getBuffer();

            for (int i = 1; i < numHashes; ++i) {
                hash = Math.abs((hashes[0] + i * hashes[1]) % NUM_BITS_PER_BLOCK);
                int byteIndex = (int) ((hash + groupStartIndex) >> 3); // divide 8
                byte b = buffer.get(byteIndex);
                int bitIndex = (int) (hash & 0x07); // mod 8
                b = (byte) (b | (1 << bitIndex));
                buffer.put(byteIndex, b);
            }
        }

        @Override
        public void end() throws HyracksDataException {
            allocateAndInitMetaDataPage();
            pageWriter.write(metaDataPage);
            for (ICachedPage p : pages) {
                pageWriter.write(p);
            }
            if (hasFailed()) {
                throw HyracksDataException.create(getFailure());
            }
            BloomFilter.this.numBits = numBits;
            BloomFilter.this.numHashes = numHashes;
            BloomFilter.this.numElements = actualNumElements;
            BloomFilter.this.numPages = numPages;
            BloomFilter.this.version = BLOCKED_BLOOM_FILTER_VERSION;
        }

        @Override
        public void abort() throws HyracksDataException {
            for (ICachedPage p : pages) {
                if (p != null) {
                    bufferCache.returnPage(p, false);
                }
            }
            if (metaDataPage != null) {
                bufferCache.returnPage(metaDataPage, false);
            }
        }

        @Override
        public void force() throws HyracksDataException {
            bufferCache.force(fileId, false);
        }
    }
}
