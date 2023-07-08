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
//import org.apache.asterix.om.lazy.metadata.stream.out.GrowableRowBytesOutputStream;
//import org.apache.asterix.om.lazy.metadata.stream.out.MultiRowTemporaryBufferBytesOutputStream;
//import org.apache.asterix.om.lazy.metadata.stream.out.ValueRowOutputStream;
//import org.apache.commons.lang3.mutable.Mutable;
//import org.apache.hyracks.api.exceptions.HyracksDataException;
//import org.apache.hyracks.data.std.api.IValueReference;
//import org.apache.parquet.bytes.BytesInput;
//import org.apache.parquet.io.ParquetEncodingException;
//
//public class ParquetRowPlainVariableLengthValuesWriter extends AbstractRowParquetValuesWriter {
//    private final GrowableRowBytesOutputStream offsetStream;
//    private final AbstractRowBytesOutputStream valueStream;
//    private final ValueRowOutputStream offsetWriterStream;
//
//    public ParquetRowPlainVariableLengthValuesWriter(Mutable<IRowWriteMultiPageOp> multiPageOpRef) {
//        offsetStream = new GrowableRowBytesOutputStream();
//        valueStream = new MultiRowTemporaryBufferBytesOutputStream(multiPageOpRef);
//        offsetWriterStream = new ValueRowOutputStream(offsetStream);
//    }
//
//    @Override
//    public void writeBytes(IValueReference v, boolean skipLengthBytes) {
//        try {
//            offsetWriterStream.writeInt(valueStream.size());
//            valueStream.write(v);
//        } catch (IOException e) {
//            throw new ParquetEncodingException("could not write bytes", e);
//        }
//    }
//
//    @Override
//    public BytesInput getBytes() {
//        try {
//            offsetStream.flush();
//            valueStream.flush();
//        } catch (IOException e) {
//            throw new ParquetEncodingException("could not write page", e);
//        }
//        return BytesInput.concat(offsetStream.asBytesInput(), valueStream.asBytesInput());
//    }
//
//    @Override
//    public void reset() throws HyracksDataException {
//        offsetStream.reset();
//        valueStream.reset();
//    }
//
//    @Override
//    public void close() {
//        offsetStream.finish();
//        valueStream.finish();
//    }
//
//    @Override
//    public int getEstimatedSize() {
//        return offsetStream.size() + valueStream.size();
//    }
//
//    @Override
//    public int getAllocatedSize() {
//        return offsetStream.capacity() + valueStream.size();
//    }
//}
