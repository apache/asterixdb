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

    private final static int NUM_BITS_PER_ELEMENT = 10;

    private final IBufferCache bufferCache;
    private final IFileMapProvider fileMapProvider;
    private final FileReference file;
    private final long numElements;
    private final int[] keyFields;
    private final int numHashes;
    private int fileId = -1;
    private boolean isActivated = false;
    private final long numPages;
    private final ArrayList<ICachedPage> bloomFilterPages = new ArrayList<ICachedPage>();
    private final static long SEED = 0L;
    private final int numBitsPerPage;

    public BloomFilter(IBufferCache bufferCache, IFileMapProvider fileMapProvider, FileReference file,
            long numElements, int[] keyFields, int numHashes) {
        this.bufferCache = bufferCache;
        this.fileMapProvider = fileMapProvider;
        this.file = file;
        this.numElements = numElements;
        this.keyFields = keyFields;
        this.numHashes = numHashes;
        numBitsPerPage = bufferCache.getPageSize() * Byte.SIZE;
        numPages = (long) Math.ceil((numElements * NUM_BITS_PER_ELEMENT) / (double) numBitsPerPage);
    }

    public int getFileId() {
        return fileId;
    }

    public FileReference getFileReference() {
        return file;
    }

    @Override
    public void add(ITupleReference tuple) {
        long[] hashes = new long[2];
        MurmurHash128Bit.hash3_x64_128(tuple, keyFields, SEED, hashes);
        for (int i = 0; i < numHashes; ++i) {
            long hash = Math.abs((hashes[0] + (long) i * hashes[1]) % numElements);

            ByteBuffer buffer = bloomFilterPages.get((int) (hash / numBitsPerPage)).getBuffer();
            int byteIndex = (int) (hash % numBitsPerPage) / Byte.SIZE;
            byte b = buffer.get(byteIndex);
            int bitIndex = (int) (hash % numBitsPerPage) % Byte.SIZE;
            b = (byte) (b | (1 << bitIndex));

            buffer.put(byteIndex, b);
        }
    }

    @Override
    public boolean contains(ITupleReference tuple) {
        long[] hashes = new long[2];
        MurmurHash128Bit.hash3_x64_128(tuple, keyFields, SEED, hashes);
        for (int i = 0; i < numHashes; ++i) {
            long hash = Math.abs((hashes[0] + (long) i * hashes[1]) % numElements);

            ByteBuffer buffer = bloomFilterPages.get((int) (hash / numBitsPerPage)).getBuffer();
            int byteIndex = (int) (hash % numBitsPerPage) / Byte.SIZE;
            byte b = buffer.get(byteIndex);
            int bitIndex = (int) (hash % numBitsPerPage) % Byte.SIZE;

            if (!((b & (1L << bitIndex)) != 0)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to create the bloom filter since it is activated.");
        }

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
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, METADATA_PAGE_ID), false);
        page.acquireWriteLatch();
        page.getBuffer().putLong(NUM_PAGES_OFFSET, numPages);
        page.getBuffer().putInt(NUM_HASHES_USED_OFFSET, numHashes);
        page.releaseWriteLatch();
        bufferCache.unpin(page);
        bufferCache.closeFile(fileId);
    }

    @Override
    public void activate() throws HyracksDataException {
        if (isActivated) {
            return;
        }

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
        isActivated = true;

        int currentPageId = 1;
        while (currentPageId <= numPages) {
            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), false);
            page.acquireWriteLatch();
            bloomFilterPages.add(page);
            ++currentPageId;
        }
    }

    @Override
    public void deactivate() throws HyracksDataException {
        if (!isActivated) {
            return;
        }

        for (int i = 0; i < numPages; ++i) {
            ICachedPage page = bloomFilterPages.get(i);
            page.releaseWriteLatch();
            bufferCache.unpin(page);
        }
        bufferCache.closeFile(fileId);
        isActivated = false;
    }

    @Override
    public void destroy() throws HyracksDataException {
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