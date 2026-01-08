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
import org.apache.asterix.column.bytes.encoder.ParquetDeltaBinaryPackingValuesWriterForInteger;
import org.apache.asterix.column.bytes.encoder.ParquetPlainFixedLengthValuesWriter;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter;
import org.apache.asterix.column.values.writer.filters.LongColumnFilterWriter;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.parquet.bytes.BytesInput;

public class IntColumnValuesWriter extends AbstractColumnValuesWriter {
    private final AbstractParquetValuesWriter intWriter;
    private final ATypeTag typeTag;

    public IntColumnValuesWriter(Mutable<IColumnWriteMultiPageOp> multiPageOpRef, int columnIndex, int level,
            boolean collection, boolean filtered, ATypeTag typeTag) {
        super(columnIndex, level, collection, filtered);
        // we might need the filtered part, as the integer primary keys are stored as Long.
        intWriter = filtered ? new ParquetDeltaBinaryPackingValuesWriterForInteger(multiPageOpRef)
                : new ParquetPlainFixedLengthValuesWriter(multiPageOpRef);

        this.typeTag = typeTag;
    }

    @Override
    protected void addValue(ATypeTag tag, IValueReference value) throws IOException {
        final int normalizedInt = getValue(tag, value.getByteArray(), value.getStartOffset());
        intWriter.writeInteger(normalizedInt);
        filterWriter.addLong(normalizedInt);
    }

    private int getValue(ATypeTag typeTag, byte[] byteArray, int offset) {
        switch (typeTag) {
            case TINYINT:
                return byteArray[offset];
            case SMALLINT:
                return ShortPointable.getShort(byteArray, offset);
            case DATE:
            case TIME:
            case YEARMONTHDURATION:
                return IntegerPointable.getInteger(byteArray, offset);
            default:
                throw new IllegalAccessError(typeTag + " is not of type integer");
        }
    }

    @Override
    protected void resetValues() throws HyracksDataException {
        intWriter.reset();
    }

    @Override
    protected BytesInput getBytes() throws IOException {
        return intWriter.getBytes();
    }

    @Override
    protected int getValuesEstimatedSize() {
        return intWriter.getEstimatedSize();
    }

    @Override
    protected int calculateEstimatedSize(int length) {
        return intWriter.calculateEstimatedSize(length);
    }

    @Override
    protected int getValuesAllocatedSize() {
        return intWriter.getAllocatedSize();
    }

    @Override
    protected void addValue(IColumnValuesReader reader) throws IOException {
        int value = reader.getInt();
        intWriter.writeInteger(value);
        filterWriter.addLong(value);
    }

    @Override
    protected AbstractColumnFilterWriter createFilter() {
        return new LongColumnFilterWriter();
    }

    @Override
    protected void closeValues() {
        intWriter.close();
    }

    @Override
    public ATypeTag getTypeTag() {
        return typeTag;
    }

    @Override
    public int getRequiredTemporaryBuffersCount() {
        return requiredTemporaryBuffers(filtered);
    }

    public static int requiredTemporaryBuffers(boolean filtered) {
        if (filtered) {
            return ParquetDeltaBinaryPackingValuesWriterForInteger.REQUIRED_TEMPORARY_BUFFERS;
        } else {
            return ParquetPlainFixedLengthValuesWriter.REQUIRED_TEMPORARY_BUFFERS;
        }
    }
}
