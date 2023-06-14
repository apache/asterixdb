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

import java.io.IOException;

import org.apache.asterix.column.bytes.stream.out.AbstractBytesOutputStream;
import org.apache.asterix.column.bytes.stream.out.GrowableBytesOutputStream;
import org.apache.asterix.column.bytes.stream.out.MultiTemporaryBufferBytesOutputStream;
import org.apache.asterix.column.bytes.stream.out.ValueOutputStream;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.io.ParquetEncodingException;

public class ParquetPlainVariableLengthValuesWriter extends AbstractParquetValuesWriter {
    private final GrowableBytesOutputStream offsetStream;
    private final AbstractBytesOutputStream valueStream;
    private final ValueOutputStream offsetWriterStream;

    public ParquetPlainVariableLengthValuesWriter(Mutable<IColumnWriteMultiPageOp> multiPageOpRef) {
        offsetStream = new GrowableBytesOutputStream();
        valueStream = new MultiTemporaryBufferBytesOutputStream(multiPageOpRef);
        offsetWriterStream = new ValueOutputStream(offsetStream);
    }

    @Override
    public void writeBytes(IValueReference v, boolean skipLengthBytes) {
        try {
            offsetWriterStream.writeInt(valueStream.size());
            valueStream.write(v);
        } catch (IOException e) {
            throw new ParquetEncodingException("could not write bytes", e);
        }
    }

    @Override
    public BytesInput getBytes() {
        try {
            offsetStream.flush();
            valueStream.flush();
        } catch (IOException e) {
            throw new ParquetEncodingException("could not write page", e);
        }
        return BytesInput.concat(offsetStream.asBytesInput(), valueStream.asBytesInput());
    }

    @Override
    public void reset() throws HyracksDataException {
        offsetStream.reset();
        valueStream.reset();
    }

    @Override
    public void close() {
        offsetStream.finish();
        valueStream.finish();
    }

    @Override
    public int getEstimatedSize() {
        return offsetStream.size() + valueStream.size();
    }

    @Override
    public int calculateEstimatedSize(int length) {
        // length of the string + 4 bytes for its offset
        return length + Integer.BYTES;
    }

    @Override
    public int getAllocatedSize() {
        return offsetStream.capacity() + valueStream.size();
    }
}
