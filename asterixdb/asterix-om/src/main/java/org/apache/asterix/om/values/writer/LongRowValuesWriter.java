///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//package org.apache.asterix.om.values.writer;
//
//import java.io.IOException;
//
//import org.apache.asterix.om.api.IRowWriteMultiPageOp;
//import org.apache.asterix.om.bytes.encoder.AbstractRowParquetValuesWriter;
//import org.apache.asterix.om.bytes.encoder.ParquetRowDeltaBinaryPackingValuesWriterForLong;
//import org.apache.asterix.om.bytes.encoder.ParquetRowPlainFixedLengthValuesWriter;
//import org.apache.asterix.om.types.ATypeTag;
//import org.apache.asterix.om.values.IRowValuesReader;
//import org.apache.asterix.om.values.writer.filters.AbstractRowFilterWriter;
//import org.apache.asterix.om.values.writer.filters.LongRowFilterWriter;
//import org.apache.commons.lang3.mutable.Mutable;
//import org.apache.hyracks.api.exceptions.HyracksDataException;
//import org.apache.hyracks.data.std.api.IValueReference;
//import org.apache.hyracks.data.std.primitive.IntegerPointable;
//import org.apache.hyracks.data.std.primitive.LongPointable;
//import org.apache.hyracks.data.std.primitive.ShortPointable;
//import org.apache.parquet.bytes.BytesInput;
//
//final class LongRowValuesWriter extends AbstractRowValuesWriter {
//    private final AbstractRowParquetValuesWriter longWriter;
//
//    public LongRowValuesWriter(Mutable<IRowWriteMultiPageOp> multiPageOpRef, int columnIndex, int level,
//            boolean collection, boolean filtered) {
//        super(columnIndex, level, collection, filtered);
//        longWriter = !filtered ? new ParquetRowPlainFixedLengthValuesWriter(multiPageOpRef)
//                : new ParquetRowDeltaBinaryPackingValuesWriterForLong(multiPageOpRef);
//    }
//
//    @Override
//    protected void addValue(ATypeTag tag, IValueReference value) throws IOException {
//        final long normalizedInt = getValue(tag, value.getByteArray(), value.getStartOffset());
//        longWriter.writeLong(normalizedInt);
//        filterWriter.addLong(normalizedInt);
//    }
//
//    private long getValue(ATypeTag typeTag, byte[] byteArray, int offset) {
//        switch (typeTag) {
//            case TINYINT:
//                return byteArray[offset];
//            case SMALLINT:
//                return ShortPointable.getShort(byteArray, offset);
//            case INTEGER:
//                return IntegerPointable.getInteger(byteArray, offset);
//            case BIGINT:
//                return LongPointable.getLong(byteArray, offset);
//            default:
//                throw new IllegalAccessError(typeTag + "is not of type integer");
//        }
//    }
//
//    @Override
//    protected void resetValues() throws HyracksDataException {
//        longWriter.reset();
//    }
//
//    @Override
//    protected BytesInput getBytes() throws IOException {
//        return longWriter.getBytes();
//    }
//
//    @Override
//    protected int getValuesEstimatedSize() {
//        return longWriter.getEstimatedSize();
//    }
//
//    @Override
//    protected int getValuesAllocatedSize() {
//        return longWriter.getAllocatedSize();
//    }
//
//    @Override
//    protected void addValue(IRowValuesReader reader) throws IOException {
//        long value = reader.getLong();
//        longWriter.writeLong(value);
//        filterWriter.addLong(value);
//    }
//
//    @Override
//    protected AbstractRowFilterWriter createFilter() {
//        return new LongRowFilterWriter();
//    }
//
//    @Override
//    protected void closeValues() {
//        longWriter.close();
//    }
//
//    @Override
//    protected ATypeTag getTypeTag() {
//        return ATypeTag.BIGINT;
//    }
//}
