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
package org.apache.hyracks.storage.am.lsm.btree.column.utils;

import org.apache.hyracks.api.compression.ICompressorDecompressor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnManager;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnMetadata;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnBTree;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentMetadata;
import org.apache.hyracks.storage.common.compression.SnappyCompressorDecompressor;
import org.apache.hyracks.util.annotations.ThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A Reader/Writer for {@link IColumnMetadata}
 */
@ThreadSafe
final class ColumnMetadataReaderWriter {
    private static final Logger LOGGER = LogManager.getLogger();
    /**
     * The header consists of two integers: [originalLength | compressedLength]
     */
    private static final int CHUNK_HEADER_SIZE = Integer.BYTES * 2;
    /**
     * Used to get the columns info from {@link IComponentMetadata#get(IValueReference, ArrayBackedValueStorage)}
     *
     * @see LSMColumnBTree#activate()
     * @see IColumnManager#activate(IValueReference)
     */
    private static final MutableArrayValueReference COLUMNS_METADATA_KEY =
            new MutableArrayValueReference("COLUMNS_METADATA".getBytes());

    /**
     * The default (and only) compressor today is 'snappy'. In the future, this could be changed.
     * Old indexes should still use snappy. But new indexes can take whatever {@link ICompressorDecompressor} passed
     * to it.
     */
    private final ICompressorDecompressor compressorDecompressor;

    /**
     * This is currently {@link ThreadSafe} since {@link SnappyCompressorDecompressor#INSTANCE} is thread safe. If the
     * {@link ICompressorDecompressor} is modified or changed, the modifier should ensure that either the new
     * {@link ICompressorDecompressor} is thread safe or the users of this class should create their own instances.
     */
    public ColumnMetadataReaderWriter() {
        compressorDecompressor = SnappyCompressorDecompressor.INSTANCE;
    }

    /**
     * Writes the metadata. If the metadata is 'large', then it will be compressed and stored in chunks
     *
     * @param metadata          to write
     * @param componentMetadata to store the metadata at
     */
    public void writeMetadata(IValueReference metadata, IComponentMetadata componentMetadata)
            throws HyracksDataException {
        int requiredLength = COLUMNS_METADATA_KEY.getLength() + metadata.getLength();
        if (componentMetadata.getAvailableSpace() >= requiredLength) {
            componentMetadata.put(COLUMNS_METADATA_KEY, metadata);
        } else {
            LOGGER.debug("Writing large column metadata of size {} bytes", requiredLength);
            writeChunks(metadata, componentMetadata);
        }
    }

    /**
     * Read the metadata. If the metadata is chunked, it will be assembled back to its original form
     *
     * @param componentMetadata source
     * @return read metadata
     */
    public IValueReference readMetadata(IComponentMetadata componentMetadata) throws HyracksDataException {
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        storage.reset();

        if (!componentMetadata.get(COLUMNS_METADATA_KEY, storage)) {
            readChunks(componentMetadata, storage);
        }

        return storage;
    }

    private void writeChunks(IValueReference metadata, IComponentMetadata componentMetadata)
            throws HyracksDataException {
        ArrayBackedValueStorage key = new ArrayBackedValueStorage(COLUMNS_METADATA_KEY.getLength() + Integer.BYTES);
        int originalLength = metadata.getLength();

        int requiredSize = compressorDecompressor.computeCompressedBufferSize(originalLength);
        ArrayBackedValueStorage compressed = new ArrayBackedValueStorage(requiredSize + CHUNK_HEADER_SIZE);

        // Write the compressed content after CHUNK_HEADER_SIZE
        int compressedLength = compressorDecompressor.compress(metadata.getByteArray(), 0, originalLength,
                compressed.getByteArray(), CHUNK_HEADER_SIZE);
        // Set the size to be the header size + compressedLength
        compressed.setSize(CHUNK_HEADER_SIZE + compressedLength);
        // Serialize the original length
        IntegerPointable.setInteger(compressed.getByteArray(), 0, originalLength);
        // Serialize the compressed length
        IntegerPointable.setInteger(compressed.getByteArray(), Integer.BYTES, compressedLength);

        // Write chunks
        VoidPointable chunk = new VoidPointable();
        int position = 0;
        int chunkId = 0;
        int keyLength = COLUMNS_METADATA_KEY.getLength() + Integer.BYTES;
        int totalLength = compressed.getLength();
        while (position < totalLength) {
            int remaining = totalLength - position;
            int freeSpace = componentMetadata.getAvailableSpace() - keyLength;
            // Find the largest chunk size that can be written
            int chunkLength = Math.min(remaining, freeSpace);
            // Prepare a chunk
            chunk.set(compressed.getByteArray(), position, chunkLength);
            // Write a chunk
            componentMetadata.put(getChunkKey(chunkId++, key), chunk);
            position += chunkLength;
        }
    }

    private void readChunks(IComponentMetadata componentMetadata, ArrayBackedValueStorage chunk)
            throws HyracksDataException {
        ArrayBackedValueStorage key = new ArrayBackedValueStorage(COLUMNS_METADATA_KEY.getLength() + Integer.BYTES);
        ArrayBackedValueStorage compressed = new ArrayBackedValueStorage();
        // Ensure large buffer to avoid enlarging the storage multiple times
        chunk.setSize(componentMetadata.getPageSize());

        int chunkId = 0;
        // Read the header + the first chunk
        chunk.reset();
        componentMetadata.get(getChunkKey(chunkId++, key), chunk);
        int originalLength = IntegerPointable.getInteger(chunk.getByteArray(), 0);
        int compressedLength = IntegerPointable.getInteger(chunk.getByteArray(), Integer.BYTES);
        // Append the first chunk without the header
        compressed.append(chunk.getByteArray(), CHUNK_HEADER_SIZE, chunk.getLength() - CHUNK_HEADER_SIZE);
        // Read the remaining chunks
        int remainingLength = compressedLength - compressed.getLength();
        while (remainingLength > 0) {
            chunk.reset();
            // Get the next chunk
            componentMetadata.get(getChunkKey(chunkId++, key), chunk);
            // Append the next chunk
            compressed.append(chunk);
            remainingLength -= chunk.getLength();
        }

        // Decompress 'compressed'
        int requiredSize = compressorDecompressor.computeCompressedBufferSize(originalLength);
        // Ensure the size
        chunk.setSize(requiredSize);
        int uncompressedLength = compressorDecompressor.uncompress(compressed.getByteArray(), 0, compressedLength,
                chunk.getByteArray(), 0);
        if (uncompressedLength != originalLength) {
            throw new IllegalStateException("Uncompressed size mismatch (original: " + originalLength
                    + ", uncompressed: " + uncompressedLength + ")");
        }

        // Set the original length
        chunk.setSize(originalLength);
    }

    private static IValueReference getChunkKey(int chunkId, ArrayBackedValueStorage storage)
            throws HyracksDataException {
        if (chunkId == 0) {
            // First chunk. Append the key prefix + set the size
            storage.reset();
            storage.append(COLUMNS_METADATA_KEY);
            storage.setSize(COLUMNS_METADATA_KEY.getLength() + Integer.BYTES);
        }

        IntegerPointable.setInteger(storage.getByteArray(), COLUMNS_METADATA_KEY.getLength(), chunkId);
        return storage;
    }
}
