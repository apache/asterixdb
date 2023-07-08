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
//package org.apache.asterix.om.bytes.encoder;
//
//import java.io.IOException;
//
//import org.apache.asterix.om.api.IRowWriteMultiPageOp;
//import org.apache.asterix.om.lazy.metadata.stream.out.AbstractRowBytesOutputStream;
//import org.apache.asterix.om.lazy.metadata.stream.out.MultiRowTemporaryBufferBytesOutputStream;
//import org.apache.asterix.om.lazy.metadata.stream.out.ValueRowOutputStream;
//import org.apache.commons.lang3.mutable.Mutable;
//import org.apache.hyracks.api.exceptions.HyracksDataException;
//import org.apache.hyracks.data.std.api.IValueReference;
//import org.apache.parquet.bytes.BytesInput;
//import org.apache.parquet.column.values.plain.PlainValuesWriter;
//import org.apache.parquet.io.ParquetEncodingException;
//
///**
// * Re-implementation of {@link PlainValuesWriter}
// */
//public class ParquetRowPlainFixedLengthValuesWriter extends AbstractRowParquetValuesWriter {
//    private final AbstractRowBytesOutputStream outputStream;
//    private final ValueRowOutputStream out;
//
//    public ParquetRowPlainFixedLengthValuesWriter(Mutable<IRowWriteMultiPageOp> multiPageOpRef) {
//        outputStream = new MultiRowTemporaryBufferBytesOutputStream(multiPageOpRef);
//        out = new ValueRowOutputStream(outputStream);
//    }
//
//    @Override
//    public void writeInteger(int v) {
//        try {
//            out.writeInt(v);
//        } catch (IOException e) {
//            throw new ParquetEncodingException("could not write int", e);
//        }
//    }
//
//    @Override
//    public void writeLong(long v) {
//        try {
//            out.writeLong(v);
//        } catch (IOException e) {
//            throw new ParquetEncodingException("could not write long", e);
//        }
//    }
//
//    @Override
//    public final void writeDouble(double v) {
//        try {
//            out.writeDouble(v);
//        } catch (IOException e) {
//            throw new ParquetEncodingException("could not write double", e);
//        }
//    }
//
//    /**
//     * Should only be used for UUID
//     *
//     * @param v               the value to encode
//     * @param skipLengthBytes ignored
//     */
//    @Override
//    public void writeBytes(IValueReference v, boolean skipLengthBytes) {
//        try {
//            out.write(v.getByteArray(), v.getStartOffset(), v.getLength());
//        } catch (IOException e) {
//            throw new ParquetEncodingException("could not write bytes", e);
//        }
//    }
//
//    @Override
//    public BytesInput getBytes() {
//        try {
//            out.flush();
//        } catch (IOException e) {
//            throw new ParquetEncodingException("could not write page", e);
//        }
//        return outputStream.asBytesInput();
//    }
//
//    @Override
//    public void reset() throws HyracksDataException {
//        outputStream.reset();
//    }
//
//    @Override
//    public void close() {
//        outputStream.finish();
//    }
//
//    @Override
//    public int getEstimatedSize() {
//        return outputStream.size();
//    }
//
//    @Override
//    public int getAllocatedSize() {
//        return outputStream.capacity();
//    }
//}
