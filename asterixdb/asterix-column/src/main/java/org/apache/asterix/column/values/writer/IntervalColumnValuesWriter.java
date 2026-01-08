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
package org.apache.asterix.column.values.writer;

import java.io.IOException;

import org.apache.asterix.column.bytes.encoder.AbstractParquetDeltaBinaryPackingValuesWriter;
import org.apache.asterix.column.bytes.encoder.ParquetDeltaBinaryPackingValuesWriterForLong;
import org.apache.asterix.column.bytes.encoder.ParquetRunLengthBitPackingHybridEncoder;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter;
import org.apache.asterix.column.values.writer.filters.IntervalColumnFilterWriter;
import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.parquet.bytes.BytesInput;

public class IntervalColumnValuesWriter extends AbstractColumnValuesWriter {
    private static final int TYPE_BIT_WIDTH = Byte.SIZE;
    private final ParquetRunLengthBitPackingHybridEncoder typeWriter;
    private final ParquetDeltaBinaryPackingValuesWriterForLong startWriter;
    private final ParquetDeltaBinaryPackingValuesWriterForLong endWriter;

    public IntervalColumnValuesWriter(Mutable<IColumnWriteMultiPageOp> multiPageOpRef, int columnIndex, int level,
            boolean collection, boolean filtered) {
        super(columnIndex, level, collection, filtered);
        typeWriter = new ParquetRunLengthBitPackingHybridEncoder(TYPE_BIT_WIDTH);
        startWriter = new ParquetDeltaBinaryPackingValuesWriterForLong(multiPageOpRef);
        endWriter = new ParquetDeltaBinaryPackingValuesWriterForLong(multiPageOpRef);
    }

    @Override
    protected AbstractColumnFilterWriter createFilter() {
        return new IntervalColumnFilterWriter();
    }

    @Override
    protected ATypeTag getTypeTag() {
        return ATypeTag.INTERVAL;
    }

    @Override
    public int getRequiredTemporaryBuffersCount() {
        return requiredTemporaryBuffers(filtered);
    }

    public static int requiredTemporaryBuffers(boolean filtered) {
        // type uses GrowableBytesOutputStream (no multi-page buffers) + 2 delta-binary-packed streams.
        return 2 * AbstractParquetDeltaBinaryPackingValuesWriter.REQUIRED_TEMPORARY_BUFFERS;
    }

    @Override
    protected void addValue(ATypeTag tag, IValueReference value) throws IOException {
        byte[] bytes = value.getByteArray();
        int offset = value.getStartOffset();
        byte intervalTimeType = AIntervalSerializerDeserializer.getIntervalTimeType(bytes, offset);
        long start = AIntervalSerializerDeserializer.getIntervalStart(bytes, offset);
        long end = AIntervalSerializerDeserializer.getIntervalEnd(bytes, offset);

        typeWriter.writeInt(intervalTimeType & 0xFF);
        startWriter.writeLong(start);
        endWriter.writeLong(end);
        filterWriter.addValue(value);
    }

    @Override
    protected void addValue(IColumnValuesReader reader) throws IOException {
        // Re-encoding from another reader (e.g., merge path): use reconstructed bytes
        addValue(ATypeTag.INTERVAL, reader.getBytes());
    }

    @Override
    protected BytesInput getBytes() throws IOException {
        BytesInput typeBytes = typeWriter.toBytes();
        BytesInput startBytes = startWriter.getBytes();
        BytesInput endBytes = endWriter.getBytes();

        int typeLen = (int) typeBytes.size();
        int startLen = (int) startBytes.size();

        return BytesInput.concat(BytesInput.fromUnsignedVarInt(typeLen), typeBytes,
                BytesInput.fromUnsignedVarInt(startLen), startBytes, endBytes);
    }

    @Override
    protected int getValuesEstimatedSize() {
        // + up to 10 bytes for two unsignedVarInt length prefixes
        return typeWriter.getEstimatedSize() + startWriter.getEstimatedSize() + endWriter.getEstimatedSize() + 10;
    }

    @Override
    protected int calculateEstimatedSize(int length) {
        // The logical interval payload is variable (1+8+8 or 1+4+4), but we store 3 numeric streams.
        return Long.BYTES * 2 + Integer.BYTES;
    }

    @Override
    protected int getValuesAllocatedSize() {
        return typeWriter.getAllocatedSize() + startWriter.getAllocatedSize() + endWriter.getAllocatedSize();
    }

    @Override
    protected void resetValues() throws org.apache.hyracks.api.exceptions.HyracksDataException {
        typeWriter.reset();
        startWriter.reset();
        endWriter.reset();
    }

    @Override
    protected void closeValues() {
        typeWriter.close();
        startWriter.close();
        endWriter.close();
    }
}
