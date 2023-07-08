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
//import org.apache.asterix.om.bytes.encoder.ParquetRowRunLengthBitPackingHybridEncoder;
//import org.apache.asterix.om.types.ATypeTag;
//import org.apache.asterix.om.values.IRowValuesReader;
//import org.apache.asterix.om.values.writer.filters.AbstractRowFilterWriter;
//import org.apache.asterix.om.values.writer.filters.LongRowFilterWriter;
//import org.apache.hyracks.data.std.api.IValueReference;
//import org.apache.parquet.bytes.BytesInput;
//
//public final class BooleanRowValuesWriter extends AbstractRowValuesWriter {
//    private final ParquetRowRunLengthBitPackingHybridEncoder booleanWriter;
//
//    public BooleanRowValuesWriter(int columnIndex, int level, boolean collection, boolean filtered) {
//        super(columnIndex, level, collection, filtered);
//        booleanWriter = new ParquetRowRunLengthBitPackingHybridEncoder(1);
//    }
//
//    @Override
//    protected void addValue(ATypeTag tag, IValueReference value) throws IOException {
//        byte booleanValue = value.getByteArray()[value.getStartOffset()];
//        booleanWriter.writeInt(booleanValue);
//        filterWriter.addLong(booleanValue);
//    }
//
//    @Override
//    protected void resetValues() {
//        booleanWriter.reset();
//    }
//
//    @Override
//    protected BytesInput getBytes() throws IOException {
//        return booleanWriter.toBytes();
//    }
//
//    @Override
//    protected int getValuesEstimatedSize() {
//        return booleanWriter.getEstimatedSize();
//    }
//
//    @Override
//    protected int getValuesAllocatedSize() {
//        return booleanWriter.getAllocatedSize();
//    }
//
//    @Override
//    protected void addValue(IRowValuesReader reader) throws IOException {
//        int value = reader.getBoolean() ? 1 : 0;
//        booleanWriter.writeInt(value);
//    }
//
//    @Override
//    protected AbstractRowFilterWriter createFilter() {
//        return new LongRowFilterWriter();
//    }
//
//    @Override
//    protected void closeValues() {
//        booleanWriter.close();
//    }
//
//    @Override
//    protected ATypeTag getTypeTag() {
//        return ATypeTag.BOOLEAN;
//    }
//}
