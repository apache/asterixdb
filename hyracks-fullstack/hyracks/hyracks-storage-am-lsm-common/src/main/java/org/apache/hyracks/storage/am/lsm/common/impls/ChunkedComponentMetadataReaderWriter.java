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
package org.apache.hyracks.storage.am.lsm.common.impls;

import org.apache.hyracks.api.compression.ICompressorDecompressor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentMetadata;
import org.apache.hyracks.storage.common.compression.SnappyCompressorDecompressor;

/**
 * Reads/writes a single component-metadata entry that may exceed one metadata page. Small values are stored as-is
 * under {@code baseKey}; values larger than the available page space are compressed and split into page-sized chunks
 * stored under derived keys ({@code baseKey || int(chunkId)}), with chunk {@code 0} carrying a
 * {@code [originalLength | compressedLength]} header.
 * <p>
 * Keys are namespaced by {@code baseKey}, so multiple spanning entries (e.g., the column metadata and the theta
 * sketch) coexist in the same component metadata without interfering, regardless of write order. The on-disk format
 * matches the original column-metadata chunking, so existing components remain readable.
 */
public final class ChunkedComponentMetadataReaderWriter {

    /** The header consists of two integers: [originalLength | compressedLength]. */
    private static final int CHUNK_HEADER_SIZE = Integer.BYTES * 2;

    private final IValueReference baseKey;
    private final ICompressorDecompressor compressorDecompressor;

    public ChunkedComponentMetadataReaderWriter(IValueReference baseKey) {
        this(baseKey, SnappyCompressorDecompressor.INSTANCE);
    }

    public ChunkedComponentMetadataReaderWriter(IValueReference baseKey,
            ICompressorDecompressor compressorDecompressor) {
        this.baseKey = baseKey;
        this.compressorDecompressor = compressorDecompressor;
    }

    /**
     * Writes {@code metadata}. If it fits the current page, it is stored as-is; otherwise it is compressed and stored
     * in chunks.
     */
    public void writeMetadata(IValueReference metadata, IComponentMetadata componentMetadata)
            throws HyracksDataException {
        int requiredLength = baseKey.getLength() + metadata.getLength();
        if (componentMetadata.getAvailableSpace() >= requiredLength) {
            componentMetadata.put(baseKey, metadata);
        } else {
            writeChunks(metadata, componentMetadata);
        }
    }

    /**
     * Reads the metadata into {@code storage}, reassembling and decompressing chunks if necessary.
     *
     * @return {@code true} if the entry is present (chunked or not), {@code false} if absent.
     */
    public boolean readMetadata(IComponentMetadata componentMetadata, ArrayBackedValueStorage storage)
            throws HyracksDataException {
        storage.reset();
        if (componentMetadata.get(baseKey, storage)) {
            // Stored as-is (fit in a single page)
            return true;
        }
        return readChunks(componentMetadata, storage);
    }

    private void writeChunks(IValueReference metadata, IComponentMetadata componentMetadata)
            throws HyracksDataException {
        ArrayBackedValueStorage key = new ArrayBackedValueStorage(baseKey.getLength() + Integer.BYTES);
        int originalLength = metadata.getLength();

        int requiredSize = compressorDecompressor.computeCompressedBufferSize(originalLength);
        ArrayBackedValueStorage compressed = new ArrayBackedValueStorage(requiredSize + CHUNK_HEADER_SIZE);

        // Write the compressed content after CHUNK_HEADER_SIZE
        int compressedLength = compressorDecompressor.compress(metadata.getByteArray(), 0, originalLength,
                compressed.getByteArray(), CHUNK_HEADER_SIZE);
        compressed.setSize(CHUNK_HEADER_SIZE + compressedLength);
        IntegerPointable.setInteger(compressed.getByteArray(), 0, originalLength);
        IntegerPointable.setInteger(compressed.getByteArray(), Integer.BYTES, compressedLength);

        VoidPointable chunk = new VoidPointable();
        int position = 0;
        int chunkId = 0;
        int keyLength = baseKey.getLength() + Integer.BYTES;
        int totalLength = compressed.getLength();
        while (position < totalLength) {
            int remaining = totalLength - position;
            int freeSpace = componentMetadata.getAvailableSpace() - keyLength;
            int chunkLength = Math.min(remaining, freeSpace);
            chunk.set(compressed.getByteArray(), position, chunkLength);
            componentMetadata.put(getChunkKey(chunkId++, key), chunk);
            position += chunkLength;
        }
    }

    private boolean readChunks(IComponentMetadata componentMetadata, ArrayBackedValueStorage result)
            throws HyracksDataException {
        ArrayBackedValueStorage key = new ArrayBackedValueStorage(baseKey.getLength() + Integer.BYTES);
        ArrayBackedValueStorage chunk = new ArrayBackedValueStorage();
        ArrayBackedValueStorage compressed = new ArrayBackedValueStorage();

        int chunkId = 0;
        chunk.reset();
        if (!componentMetadata.get(getChunkKey(chunkId++, key), chunk)) {
            // Neither a plain entry nor a chunked entry exists.
            return false;
        }
        int originalLength = IntegerPointable.getInteger(chunk.getByteArray(), 0);
        int compressedLength = IntegerPointable.getInteger(chunk.getByteArray(), Integer.BYTES);
        compressed.append(chunk.getByteArray(), CHUNK_HEADER_SIZE, chunk.getLength() - CHUNK_HEADER_SIZE);
        int remainingLength = compressedLength - compressed.getLength();
        while (remainingLength > 0) {
            chunk.reset();
            componentMetadata.get(getChunkKey(chunkId++, key), chunk);
            compressed.append(chunk);
            remainingLength -= chunk.getLength();
        }

        result.reset();
        result.setSize(Math.max(originalLength, compressorDecompressor.computeCompressedBufferSize(originalLength)));
        int uncompressedLength = compressorDecompressor.uncompress(compressed.getByteArray(), 0, compressedLength,
                result.getByteArray(), 0);
        if (uncompressedLength != originalLength) {
            throw new IllegalStateException("Uncompressed size mismatch (original: " + originalLength
                    + ", uncompressed: " + uncompressedLength + ")");
        }
        result.setSize(originalLength);
        return true;
    }

    private IValueReference getChunkKey(int chunkId, ArrayBackedValueStorage storage) throws HyracksDataException {
        if (chunkId == 0) {
            // First chunk: append the key prefix + reserve space for the chunk id
            storage.reset();
            storage.append(baseKey);
            storage.setSize(baseKey.getLength() + Integer.BYTES);
        }
        IntegerPointable.setInteger(storage.getByteArray(), baseKey.getLength(), chunkId);
        return storage;
    }
}
