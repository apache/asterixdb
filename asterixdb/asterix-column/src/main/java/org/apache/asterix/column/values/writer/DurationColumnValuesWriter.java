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

import org.apache.asterix.column.bytes.encoder.ParquetDeltaBinaryPackingValuesWriterForInteger;
import org.apache.asterix.column.bytes.encoder.ParquetDeltaBinaryPackingValuesWriterForLong;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter;
import org.apache.asterix.column.values.writer.filters.DurationColumnFilterWriter;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.parquet.bytes.BytesInput;

/**
 * Column values writer for {@link org.apache.asterix.om.types.ATypeTag#DURATION}.
 * <p>
 * Asterix {@code DURATION} is a fixed-width composite value (months:int32 + milliseconds:int64), i.e., 12 bytes.
 * <p>
 * For better compression than treating the 12 bytes as opaque, we store it as two delta-binary-packed numeric
 * streams:
 * <ul>
 *   <li>months: int32 stream</li>
 *   <li>milliseconds: int64 stream</li>
 * </ul>
 * The encoded bytes layout is:
 * <pre>
 *   unsignedVarInt monthsStreamLength
 *   monthsStreamBytes
 *   millisStreamBytes
 * </pre>
 * <p>
 * NOTE: Since the columnar page-zero filter stores only two 8-byte longs, we cannot safely create an
 * order-preserving single-long min/max representation for DURATION. This writer therefore stores the values, but
 * uses a conservative filter that avoids pruning based on DURATION filters.
 */
public class DurationColumnValuesWriter extends AbstractColumnValuesWriter {
    private static final int MONTHS_OFFSET = 0;
    private static final int MILLIS_OFFSET = Integer.BYTES;
    private static final int NON_TAGGED_DURATION_LENGTH = Integer.BYTES + Long.BYTES;

    private final ParquetDeltaBinaryPackingValuesWriterForInteger monthsWriter;
    private final ParquetDeltaBinaryPackingValuesWriterForLong millisWriter;

    public DurationColumnValuesWriter(Mutable<IColumnWriteMultiPageOp> multiPageOpRef, int columnIndex, int level,
            boolean collection, boolean filtered) {
        super(columnIndex, level, collection, filtered);
        monthsWriter = new ParquetDeltaBinaryPackingValuesWriterForInteger(multiPageOpRef);
        millisWriter = new ParquetDeltaBinaryPackingValuesWriterForLong(multiPageOpRef);
    }

    @Override
    protected AbstractColumnFilterWriter createFilter() {
        return new DurationColumnFilterWriter();
    }

    @Override
    protected ATypeTag getTypeTag() {
        return ATypeTag.DURATION;
    }

    @Override
    public int getRequiredTemporaryBuffersCount() {
        return requiredTemporaryBuffers(filtered);
    }

    public static int requiredTemporaryBuffers(boolean filtered) {
        // Two delta-binary-packed streams.
        return ParquetDeltaBinaryPackingValuesWriterForInteger.REQUIRED_TEMPORARY_BUFFERS
                + ParquetDeltaBinaryPackingValuesWriterForLong.REQUIRED_TEMPORARY_BUFFERS;
    }

    @Override
    protected void addValue(ATypeTag tag, IValueReference value) throws IOException {
        byte[] bytes = value.getByteArray();
        int offset = value.getStartOffset();
        int months = IntegerPointable.getInteger(bytes, offset + MONTHS_OFFSET);
        long millis = LongPointable.getLong(bytes, offset + MILLIS_OFFSET);
        monthsWriter.writeInteger(months);
        millisWriter.writeLong(millis);
        //        filterWriter.addValue(value);
    }

    @Override
    protected void addValue(IColumnValuesReader reader) throws IOException {
        IValueReference value = reader.getBytes();
        byte[] bytes = value.getByteArray();
        int offset = value.getStartOffset();
        int months = IntegerPointable.getInteger(bytes, offset + MONTHS_OFFSET);
        long millis = LongPointable.getLong(bytes, offset + MILLIS_OFFSET);
        monthsWriter.writeInteger(months);
        millisWriter.writeLong(millis);
        //        filterWriter.addValue(value);
    }

    @Override
    protected BytesInput getBytes() throws IOException {
        BytesInput monthsBytes = monthsWriter.getBytes();
        BytesInput millisBytes = millisWriter.getBytes();
        int monthsLen = (int) monthsBytes.size();
        BytesInput monthsLenBytes = BytesInput.fromUnsignedVarInt(monthsLen);
        return BytesInput.concat(monthsLenBytes, monthsBytes, millisBytes);
    }

    @Override
    protected int getValuesEstimatedSize() {
        return monthsWriter.getEstimatedSize() + millisWriter.getEstimatedSize() + Integer.BYTES;
    }

    @Override
    protected int calculateEstimatedSize(int length) {
        return NON_TAGGED_DURATION_LENGTH;
    }

    @Override
    protected int getValuesAllocatedSize() {
        return monthsWriter.getAllocatedSize() + millisWriter.getAllocatedSize();
    }

    @Override
    protected void resetValues() throws HyracksDataException {
        monthsWriter.reset();
        millisWriter.reset();
    }

    @Override
    protected void closeValues() {
        monthsWriter.close();
        millisWriter.close();
    }
}
