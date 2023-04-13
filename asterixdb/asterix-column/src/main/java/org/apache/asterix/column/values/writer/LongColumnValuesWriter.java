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

import org.apache.asterix.column.bytes.encoder.AbstractParquetValuesWriter;
import org.apache.asterix.column.bytes.encoder.ParquetDeltaBinaryPackingValuesWriterForLong;
import org.apache.asterix.column.bytes.encoder.ParquetPlainFixedLengthValuesWriter;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter;
import org.apache.asterix.column.values.writer.filters.LongColumnFilterWriter;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.parquet.bytes.BytesInput;

final class LongColumnValuesWriter extends AbstractColumnValuesWriter {
    private final AbstractParquetValuesWriter longWriter;

    public LongColumnValuesWriter(Mutable<IColumnWriteMultiPageOp> multiPageOpRef, int columnIndex, int level,
            boolean collection, boolean filtered) {
        super(columnIndex, level, collection, filtered);
        longWriter = !filtered ? new ParquetPlainFixedLengthValuesWriter(multiPageOpRef)
                : new ParquetDeltaBinaryPackingValuesWriterForLong(multiPageOpRef);
    }

    @Override
    protected void addValue(ATypeTag tag, IValueReference value) throws IOException {
        final long normalizedInt = getValue(tag, value.getByteArray(), value.getStartOffset());
        longWriter.writeLong(normalizedInt);
        filterWriter.addLong(normalizedInt);
    }

    private long getValue(ATypeTag typeTag, byte[] byteArray, int offset) {
        switch (typeTag) {
            case TINYINT:
                return byteArray[offset];
            case SMALLINT:
                return ShortPointable.getShort(byteArray, offset);
            case INTEGER:
                return IntegerPointable.getInteger(byteArray, offset);
            case BIGINT:
                return LongPointable.getLong(byteArray, offset);
            default:
                throw new IllegalAccessError(typeTag + "is not of type integer");
        }
    }

    @Override
    protected void resetValues() throws HyracksDataException {
        longWriter.reset();
    }

    @Override
    protected BytesInput getBytes() throws IOException {
        return longWriter.getBytes();
    }

    @Override
    protected int getValuesEstimatedSize() {
        return longWriter.getEstimatedSize();
    }

    @Override
    protected int getValuesAllocatedSize() {
        return longWriter.getAllocatedSize();
    }

    @Override
    protected void addValue(IColumnValuesReader reader) throws IOException {
        long value = reader.getLong();
        longWriter.writeLong(value);
        filterWriter.addLong(value);
    }

    @Override
    protected AbstractColumnFilterWriter createFilter() {
        return new LongColumnFilterWriter();
    }

    @Override
    protected void closeValues() {
        longWriter.close();
    }

    @Override
    protected ATypeTag getTypeTag() {
        return ATypeTag.BIGINT;
    }
}
