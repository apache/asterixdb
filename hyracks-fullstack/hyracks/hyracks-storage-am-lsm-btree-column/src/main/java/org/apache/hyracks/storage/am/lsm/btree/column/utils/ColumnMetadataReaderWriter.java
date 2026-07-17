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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnManager;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnMetadata;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnBTree;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.impls.ChunkedComponentMetadataReaderWriter;
import org.apache.hyracks.storage.common.compression.SnappyCompressorDecompressor;
import org.apache.hyracks.util.annotations.ThreadSafe;

/**
 * A Reader/Writer for {@link IColumnMetadata}. Large metadata is compressed and chunked across metadata pages; the
 * actual storage format is handled by {@link ChunkedComponentMetadataReaderWriter}.
 */
@ThreadSafe
final class ColumnMetadataReaderWriter {
    /**
     * Used to get the columns info from {@link IComponentMetadata#get(IValueReference, ArrayBackedValueStorage)}
     *
     * @see LSMColumnBTree#activate()
     * @see IColumnManager#activate(IValueReference)
     */
    private static final MutableArrayValueReference COLUMNS_METADATA_KEY =
            new MutableArrayValueReference("COLUMNS_METADATA".getBytes());

    /**
     * The default (and only) compressor today is 'snappy'. {@link SnappyCompressorDecompressor#INSTANCE} is thread
     * safe, which keeps this class {@link ThreadSafe}.
     */
    private final ChunkedComponentMetadataReaderWriter readerWriter;

    public ColumnMetadataReaderWriter() {
        readerWriter =
                new ChunkedComponentMetadataReaderWriter(COLUMNS_METADATA_KEY, SnappyCompressorDecompressor.INSTANCE);
    }

    /**
     * Writes the metadata. If the metadata is 'large', then it will be compressed and stored in chunks.
     *
     * @param metadata          to write
     * @param componentMetadata to store the metadata at
     */
    public void writeMetadata(IValueReference metadata, IComponentMetadata componentMetadata)
            throws HyracksDataException {
        readerWriter.writeMetadata(metadata, componentMetadata);
    }

    /**
     * Read the metadata. If the metadata is chunked, it will be assembled back to its original form.
     *
     * @param componentMetadata source
     * @return read metadata
     */
    public IValueReference readMetadata(IComponentMetadata componentMetadata) throws HyracksDataException {
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        readerWriter.readMetadata(componentMetadata, storage);
        return storage;
    }
}
