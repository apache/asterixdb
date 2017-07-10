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
import org.apache.hyracks.storage.common.buffercache.IFIFOPageQueue;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class BloomFilter {

    private static final int METADATA_PAGE_ID = 0;
    private static final int NUM_PAGES_OFFSET = 0; // 0
    private static final int NUM_HASHES_USED_OFFSET = NUM_PAGES_OFFSET + 4; // 4
    private static final int NUM_ELEMENTS_OFFSET = NUM_HASHES_USED_OFFSET + 4; // 8
    private static final int NUM_BITS_OFFSET = NUM_ELEMENTS_OFFSET + 8; // 12

    private final IBufferCache bufferCache;
    private final FileReference file;
    private final int[] keyFields;
    private int fileId = -1;
    private boolean isActivated = false;

    private int numPages;
    private int numHashes;
    private long numElements;
    private long numBits;
    private final int numBitsPerPage;
    private static final byte[] ZERO_BUFFER = new byte[131072]; // 128kb
    private static final long SEED = 0L;

    public BloomFilter(IBufferCache bufferCache, FileReference file, int[] keyFields) throws HyracksDataException {
        this.bufferCache = bufferCache;
        this.file = file;
        this.keyFields = keyFields;
        this.numBitsPerPage = bufferCache.getPageSize() * Byte.SIZE;
    }

    public int getFileId() {
        return fileId;
    }

    public FileReference getFileReference() {
        return file;
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
            return;
        }
        ICachedPage metaPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, METADATA_PAGE_ID), false);
        metaPage.acquireReadLatch();
        try {
            numPages = metaPage.getBuffer().getInt(NUM_PAGES_OFFSET);
            numHashes = metaPage.getBuffer().getInt(NUM_HASHES_USED_OFFSET);
            numElements = metaPage.getBuffer().getLong(NUM_ELEMENTS_OFFSET);
            numBits = metaPage.getBuffer().getLong(NUM_BITS_OFFSET);
        } finally {
            metaPage.releaseReadLatch();
            bufferCache.unpin(metaPage);
        }
    }

    public synchronized void deactivate() throws HyracksDataException {
        if (!isActivated) {
            throw HyracksDataException.create(ErrorCode.CANNOT_DEACTIVATE_INACTIVE_BLOOM_FILTER);
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

    public synchronized void destroy() throws HyracksDataException {
        if (isActivated) {
            throw HyracksDataException.create(ErrorCode.CANNOT_DESTROY_ACTIVE_BLOOM_FILTER);
        }
        bufferCache.deleteFile(file);
    }

    public IIndexBulkLoader createBuilder(long numElements, int numHashes, int numBitsPerElement)
            throws HyracksDataException {
        return new BloomFilterBuilder(numElements, numHashes, numBitsPerElement);
    }

    public class BloomFilterBuilder implements IIndexBulkLoader {
        private final long[] hashes = new long[2];
        private final long numElements;
        private final int numHashes;
        private final long numBits;
        private final int numPages;
        private IFIFOPageQueue queue;
        private ICachedPage[] pages;
        private ICachedPage metaDataPage = null;

        public BloomFilterBuilder(long numElements, int numHashes, int numBitsPerElement) throws HyracksDataException {
            if (!isActivated) {
                throw HyracksDataException.create(ErrorCode.CANNOT_CREATE_BLOOM_FILTER_BUILDER_FOR_INACTIVE_FILTER);
            }
            queue = bufferCache.createFIFOQueue();
            this.numElements = numElements;
            this.numHashes = numHashes;
            numBits = this.numElements * numBitsPerElement;
            long tmp = (long) Math.ceil(numBits / (double) numBitsPerPage);
            if (tmp > Integer.MAX_VALUE) {
                throw HyracksDataException.create(ErrorCode.CANNOT_CREATE_BLOOM_FILTER_WITH_NUMBER_OF_PAGES, tmp);
            }
            numPages = (int) tmp;
            pages = new ICachedPage[numPages];
            int currentPageId = 1;
            while (currentPageId <= numPages) {
                ICachedPage page = bufferCache.confiscatePage(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
                initPage(page.getBuffer().array());
                pages[currentPageId - 1] = page;
                ++currentPageId;
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
            metaDataPage.getBuffer().putLong(NUM_ELEMENTS_OFFSET, numElements);
            metaDataPage.getBuffer().putLong(NUM_BITS_OFFSET, numBits);
        }

        @Override
        public void add(ITupleReference tuple) throws HyracksDataException {
            if (numPages == 0) {
                throw HyracksDataException.create(ErrorCode.CANNOT_ADD_TUPLES_TO_DUMMY_BLOOM_FILTER);
            }
            MurmurHash128Bit.hash3_x64_128(tuple, keyFields, SEED, hashes);
            for (int i = 0; i < numHashes; ++i) {
                long hash = Math.abs((hashes[0] + i * hashes[1]) % numBits);
                ICachedPage page = pages[(int) (hash / numBitsPerPage)];
                ByteBuffer buffer = page.getBuffer();
                int byteIndex = (int) (hash % numBitsPerPage) >> 3; // divide by 8
                byte b = buffer.get(byteIndex);
                int bitIndex = (int) (hash % numBitsPerPage) & 0x07; // mod 8
                b = (byte) (b | (1 << bitIndex));

                buffer.put(byteIndex, b);
            }
        }

        @Override
        public void end() throws HyracksDataException {
            allocateAndInitMetaDataPage();
            queue.put(metaDataPage);
            for (ICachedPage p : pages) {
                queue.put(p);
            }
            bufferCache.finishQueue();
            BloomFilter.this.numBits = numBits;
            BloomFilter.this.numHashes = numHashes;
            BloomFilter.this.numElements = numElements;
            BloomFilter.this.numPages = numPages;
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

    }
}
