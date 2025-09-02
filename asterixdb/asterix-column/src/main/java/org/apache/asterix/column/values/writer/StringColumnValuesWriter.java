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
import org.apache.asterix.column.bytes.encoder.ParquetDeltaByteArrayWriter;
import org.apache.asterix.column.bytes.encoder.ParquetPlainVariableLengthValuesWriter;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter;
import org.apache.asterix.column.values.writer.filters.StringColumnFilterWriter;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.parquet.bytes.BytesInput;

public class StringColumnValuesWriter extends AbstractColumnValuesWriter {
    private final AbstractParquetValuesWriter stringWriter;
    private final boolean skipLengthBytes;

    public StringColumnValuesWriter(Mutable<IColumnWriteMultiPageOp> multiPageOpRef, int columnIndex, int level,
            boolean collection, boolean filtered) {
        this(columnIndex, level, collection, filtered, true, filtered ? new ParquetDeltaByteArrayWriter(multiPageOpRef)
                : new ParquetPlainVariableLengthValuesWriter(multiPageOpRef));
    }

    protected StringColumnValuesWriter(int columnIndex, int level, boolean collection, boolean filtered,
            boolean skipLengthBytes, AbstractParquetValuesWriter stringWriter) {
        super(columnIndex, level, collection, filtered);
        this.stringWriter = stringWriter;
        this.skipLengthBytes = skipLengthBytes;
    }

    @Override
    protected final void addValue(ATypeTag tag, IValueReference value) throws IOException {
        stringWriter.writeBytes(value, skipLengthBytes);
        filterWriter.addValue(value);
    }

    @Override
    protected final void resetValues() throws HyracksDataException {
        stringWriter.reset();
    }

    @Override
    protected final BytesInput getBytes() throws IOException {
        return stringWriter.getBytes();
    }

    @Override
    protected final int getValuesEstimatedSize() {
        return stringWriter.getEstimatedSize();
    }

    @Override
    protected int calculateEstimatedSize(int length) {
        return stringWriter.calculateEstimatedSize(length);
    }

    @Override
    protected final int getValuesAllocatedSize() {
        return stringWriter.getAllocatedSize();
    }

    @Override
    protected final void addValue(IColumnValuesReader reader) throws IOException {
        IValueReference value = reader.getBytes();
        stringWriter.writeBytes(value, skipLengthBytes);
        filterWriter.addValue(value);
    }

    @Override
    protected AbstractColumnFilterWriter createFilter() {
        return new StringColumnFilterWriter();
    }

    @Override
    protected final void closeValues() {
        stringWriter.close();
    }

    @Override
    protected ATypeTag getTypeTag() {
        return ATypeTag.STRING;
    }

    @Override
    public int getRequiredTemporaryBuffersCount() {
        return requiredTemporaryBuffers(filtered);
    }

    public static int requiredTemporaryBuffers(boolean filtered) {
        if (filtered) {
            return ParquetDeltaByteArrayWriter.REQUIRED_TEMPORARY_BUFFERS;
        } else {
            return ParquetPlainVariableLengthValuesWriter.REQUIRED_TEMPORARY_BUFFERS;
        }
    }
}
