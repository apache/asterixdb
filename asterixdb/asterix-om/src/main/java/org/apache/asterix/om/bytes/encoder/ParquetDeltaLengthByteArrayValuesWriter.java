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
//import org.apache.asterix.om.lazy.metadata.stream.out.MultiRowTemporaryBufferBytesOutputStream;
//import org.apache.commons.lang3.mutable.Mutable;
//import org.apache.hyracks.api.exceptions.HyracksDataException;
//import org.apache.hyracks.data.std.api.IValueReference;
//import org.apache.parquet.bytes.BytesInput;
//import org.apache.parquet.bytes.LittleEndianDataOutputStream;
//import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
//import org.apache.parquet.io.ParquetEncodingException;
//
///**
// * Re-implementation of {@link DeltaLengthByteArrayValuesWriter}
// */
//public class ParquetDeltaLengthByteArrayValuesWriter extends AbstractRowParquetValuesWriter {
//    private final ParquetRowDeltaBinaryPackingValuesWriterForInteger lengthWriter;
//    private final MultiRowTemporaryBufferBytesOutputStream outputStream;
//    private final LittleEndianDataOutputStream out;
//
//    public ParquetDeltaLengthByteArrayValuesWriter(Mutable<IRowWriteMultiPageOp> multiPageOpRef) {
//        outputStream = new MultiRowTemporaryBufferBytesOutputStream(multiPageOpRef);
//        out = new LittleEndianDataOutputStream(outputStream);
//        lengthWriter = new ParquetRowDeltaBinaryPackingValuesWriterForInteger(multiPageOpRef);
//    }
//
//    @Override
//    public void writeBytes(IValueReference value, boolean skipLengthBytes) {
//        try {
//            lengthWriter.writeInteger(value.getLength());
//            out.write(value.getByteArray(), value.getStartOffset(), value.getLength());
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
//        BytesInput lengthBytes = lengthWriter.getBytes();
//        BytesInput lengthSize = BytesInput.fromUnsignedVarInt((int) lengthBytes.size());
//        BytesInput arrayBytes = outputStream.asBytesInput();
//        return BytesInput.concat(lengthSize, lengthBytes, arrayBytes);
//    }
//
//    @Override
//    public void reset() throws HyracksDataException {
//        lengthWriter.reset();
//        outputStream.reset();
//    }
//
//    @Override
//    public void close() {
//        lengthWriter.close();
//        outputStream.finish();
//    }
//
//    @Override
//    public int getEstimatedSize() {
//        return lengthWriter.getEstimatedSize() + outputStream.size();
//    }
//
//    @Override
//    public int getAllocatedSize() {
//        return lengthWriter.getAllocatedSize() + outputStream.capacity();
//    }
//}
