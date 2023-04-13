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
package org.apache.asterix.column.bytes.encoder;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;

/**
 * Re-implementation of {@link DeltaByteArrayWriter}
 */
public class ParquetDeltaByteArrayWriter extends AbstractParquetValuesWriter {
    private static final IValueReference EMPTY_VALUE;
    private final ParquetDeltaBinaryPackingValuesWriterForInteger prefixLengthWriter;
    private final ParquetDeltaLengthByteArrayValuesWriter suffixWriter;
    private final VoidPointable suffix;
    private final ArrayBackedValueStorage previous = new ArrayBackedValueStorage();

    static {
        VoidPointable emptyPointable = new VoidPointable();
        emptyPointable.set(new byte[0], 0, 0);
        EMPTY_VALUE = emptyPointable;
    }

    public ParquetDeltaByteArrayWriter(Mutable<IColumnWriteMultiPageOp> multiPageOpRef) {
        this.prefixLengthWriter = new ParquetDeltaBinaryPackingValuesWriterForInteger(multiPageOpRef);
        this.suffixWriter = new ParquetDeltaLengthByteArrayValuesWriter(multiPageOpRef);
        suffix = new VoidPointable();
        suffix.set(EMPTY_VALUE);
    }

    @Override
    public BytesInput getBytes() {
        BytesInput prefixBytes = prefixLengthWriter.getBytes();
        BytesInput prefixLength = BytesInput.fromUnsignedVarInt((int) prefixBytes.size());
        BytesInput suffixBytes = suffixWriter.getBytes();
        return BytesInput.concat(prefixLength, prefixBytes, suffixBytes);
    }

    @Override
    public void reset() throws HyracksDataException {
        prefixLengthWriter.reset();
        suffixWriter.reset();
        previous.reset();
        suffix.set(EMPTY_VALUE);
    }

    @Override
    public void close() {
        prefixLengthWriter.close();
        suffixWriter.close();
        previous.reset();
        suffix.set(EMPTY_VALUE);
    }

    @Override
    public int getEstimatedSize() {
        return prefixLengthWriter.getEstimatedSize() + suffixWriter.getEstimatedSize();
    }

    @Override
    public int getAllocatedSize() {
        return prefixLengthWriter.getAllocatedSize() + suffixWriter.getAllocatedSize();
    }

    @Override
    public void writeBytes(IValueReference value, boolean skipLengthBytes) {
        byte[] bytes = value.getByteArray();
        int start = value.getStartOffset();
        int length = value.getLength();
        if (skipLengthBytes) {
            // Length bytes are skipped so the prefix encoding works properly (e.g., "123", "1234")
            // the prefix "123" is a common substring between the two; however, their lengths are not
            int lengthBytes = UTF8StringUtil.getNumBytesToStoreLength(bytes, start);
            start += lengthBytes;
            length -= lengthBytes;
        }
        writeBytes(bytes, start, length);
    }

    private void writeBytes(byte[] bytes, int offset, int length) {
        final byte[] prevBytes = previous.getByteArray();
        final int prevOffset = previous.getStartOffset();
        final int minLength = Math.min(length, previous.getLength());
        // find the number of matching prefix bytes between this value and the previous one
        int i;
        for (i = 0; (i < minLength) && (bytes[i + offset] == prevBytes[i + prevOffset]); i++);
        prefixLengthWriter.writeInteger(i);
        suffix.set(bytes, offset + i, length - i);
        suffixWriter.writeBytes(suffix, false);
        // We store as bytes could be evicted from the buffer cache
        previous.set(bytes, offset, length);
    }
}
