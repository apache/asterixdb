/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.bloomfilter.impls;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.bloomfilter.api.IFilter;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class BloomFilter implements IFilter {

    private final static int METADATA_PAGE_ID = 0;
    private final static int NUM_PAGES_OFFSET = 0; // 0
    private final static int NUM_HASHES_USED_OFFSET = NUM_PAGES_OFFSET + 8; // 8
    private final static int NUM_BITS = NUM_HASHES_USED_OFFSET + 4; // 12

    private final static int NUM_BITS_PER_ELEMENT = 10;

    private final IBufferCache bufferCache;
    private final IFileMapProvider fileMapProvider;
    private final FileReference file;
    private final int[] keyFields;
    private int numHashes;
    private int fileId = -1;
    private boolean isActivated = false;
    private long numPages;

    private long numBits;
    private int numBitsPerPage;

    private final ArrayList<ICachedPage> bloomFilterPages = new ArrayList<ICachedPage>();
    private final static long SEED = 0L;

    private final boolean isNewFilter;

    public BloomFilter(IBufferCache bufferCache, IFileMapProvider fileMapProvider, FileReference file, int[] keyFields,
            long numElements, int numHashes) throws HyracksDataException {
        this.bufferCache = bufferCache;
        this.fileMapProvider = fileMapProvider;
        this.file = file;
        this.keyFields = keyFields;
        this.numHashes = numHashes;
        numBitsPerPage = bufferCache.getPageSize() * Byte.SIZE;
        numBits = numElements * NUM_BITS_PER_ELEMENT;
        numPages = (long) Math.ceil(numBits / (double) numBitsPerPage);
        isNewFilter = true;
    }

    public BloomFilter(IBufferCache bufferCache, IFileMapProvider fileMapProvider, FileReference file, int[] keyFields)
            throws HyracksDataException {
        this.bufferCache = bufferCache;
        this.fileMapProvider = fileMapProvider;
        this.file = file;
        this.keyFields = keyFields;
        numBitsPerPage = bufferCache.getPageSize() * Byte.SIZE;
        isNewFilter = false;
    }

    public int getFileId() {
        return fileId;
    }

    public FileReference getFileReference() {
        return file;
    }

    @Override
    public void add(ITupleReference tuple, long[] hashes) {
        MurmurHash128Bit.hash3_x64_128(tuple, keyFields, SEED, hashes);
        for (int i = 0; i < numHashes; ++i) {
            long hash = Math.abs((hashes[0] + (long) i * hashes[1]) % numBits);

            ByteBuffer buffer = bloomFilterPages.get((int) (hash / numBitsPerPage)).getBuffer();
            int byteIndex = (int) (hash % numBitsPerPage) >> 3; // divide by 8
            byte b = buffer.get(byteIndex);
            int bitIndex = (int) (hash % numBitsPerPage) & 0x07; // mod 8
            b = (byte) (b | (1 << bitIndex));

            buffer.put(byteIndex, b);
        }
    }

    @Override
    public boolean contains(ITupleReference tuple, long[] hashes) {
        MurmurHash128Bit.hash3_x64_128(tuple, keyFields, SEED, hashes);
        for (int i = 0; i < numHashes; ++i) {
            long hash = Math.abs((hashes[0] + (long) i * hashes[1]) % numBits);

            ByteBuffer buffer = bloomFilterPages.get((int) (hash / numBitsPerPage)).getBuffer();
            int byteIndex = (int) (hash % numBitsPerPage) >> 3; // divide by 8
            byte b = buffer.get(byteIndex);
            int bitIndex = (int) (hash % numBitsPerPage) & 0x07; // mod 8

            if (!((b & (1L << bitIndex)) != 0)) {
                return false;
            }
        }
        return true;
    }

    private void prepareFile() throws HyracksDataException {
        boolean fileIsMapped = false;
        synchronized (fileMapProvider) {
            fileIsMapped = fileMapProvider.isMapped(file);
            if (!fileIsMapped) {
                bufferCache.createFile(file);
            }
            fileId = fileMapProvider.lookupFileId(file);
            try {
                // Also creates the file if it doesn't exist yet.
                bufferCache.openFile(fileId);
            } catch (HyracksDataException e) {
                // Revert state of buffer cache since file failed to open.
                if (!fileIsMapped) {
                    bufferCache.deleteFile(fileId, false);
                }
                throw e;
            }
        }
    }

    private void persistBloomFilterMetaData() throws HyracksDataException {
        ICachedPage metaPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, METADATA_PAGE_ID), false);
        metaPage.acquireWriteLatch();
        metaPage.getBuffer().putLong(NUM_PAGES_OFFSET, numPages);
        metaPage.getBuffer().putInt(NUM_HASHES_USED_OFFSET, numHashes);
        metaPage.getBuffer().putLong(NUM_BITS, numBits);
        metaPage.releaseWriteLatch();
        bufferCache.unpin(metaPage);
    }

    private void readBloomFilterMetaData() throws HyracksDataException {
        ICachedPage metaPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, METADATA_PAGE_ID), false);
        metaPage.acquireReadLatch();
        numPages = metaPage.getBuffer().getLong(NUM_PAGES_OFFSET);
        numHashes = metaPage.getBuffer().getInt(NUM_HASHES_USED_OFFSET);
        numBits = metaPage.getBuffer().getLong(NUM_BITS);
        metaPage.releaseReadLatch();
        bufferCache.unpin(metaPage);
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to create the bloom filter since it is activated.");
        } else if (!isNewFilter) {
            throw new HyracksDataException(
                    "Cannot create a bloom filter with this bloom filter instance. Please use a different bloom filter constructor.");
        }

        prepareFile();
        persistBloomFilterMetaData();
        bufferCache.closeFile(fileId);
    }

    @Override
    public synchronized void activate() throws HyracksDataException {
        if (isActivated) {
            return;
        }

        prepareFile();
        readBloomFilterMetaData();

        int currentPageId = 1;
        while (currentPageId <= numPages) {
            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), false);
            page.acquireWriteLatch();
            bloomFilterPages.add(page);
            ++currentPageId;
        }
        isActivated = true;
    }

    @Override
    public synchronized void deactivate() throws HyracksDataException {
        if (!isActivated) {
            return;
        }

        for (int i = 0; i < numPages; ++i) {
            ICachedPage page = bloomFilterPages.get(i);
            page.releaseWriteLatch();
            bufferCache.unpin(page);
        }
        bloomFilterPages.clear();
        bufferCache.closeFile(fileId);
        isActivated = false;
    }

    @Override
    public synchronized void destroy() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to destroy the bloom filter since it is activated.");
        }

        file.delete();
        if (fileId == -1) {
            return;
        }
        bufferCache.deleteFile(fileId, false);
        fileId = -1;
    }
}