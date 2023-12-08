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
//package org.apache.asterix.om.bytes.decoder;
//
//import java.io.EOFException;
//import java.io.IOException;
//
//import org.apache.asterix.om.lazy.metadata.stream.in.AbstractRowBytesInputStream;
//import org.apache.asterix.om.lazy.metadata.stream.in.ValueRowInputStream;
//import org.apache.hyracks.data.std.api.IPointable;
//import org.apache.hyracks.data.std.api.IValueReference;
//import org.apache.parquet.io.ParquetDecodingException;
//
//public class ParquetRowPlainFixedLengthValuesReader extends AbstractRowParquetValuesReader {
//    private final ValueRowInputStream in;
//    private final int valueLength;
//    private final IPointable valueStorage;
//
//    public ParquetRowPlainFixedLengthValuesReader(int valueLength) {
//        in = new ValueRowInputStream();
//        this.valueLength = valueLength;
//        valueStorage = null;
//    }
//
//    public ParquetRowPlainFixedLengthValuesReader(IPointable valueStorage) {
//        in = new ValueRowInputStream();
//        this.valueLength = valueStorage.getByteArray().length;
//        this.valueStorage = valueStorage;
//    }
//
//    @Override
//    public void initFromPage(AbstractRowBytesInputStream stream) throws EOFException {
//        in.reset(stream.remainingStream());
//    }
//
//    @Override
//    public void skip() {
//        try {
//            in.skipBytes(valueLength);
//        } catch (IOException e) {
//            throw new ParquetDecodingException("could not skip double", e);
//        }
//    }
//
//    @Override
//    public long readLong() {
//        try {
//            return in.readLong();
//        } catch (IOException e) {
//            throw new ParquetDecodingException("could not read double", e);
//        }
//    }
//
//    @Override
//    public double readDouble() {
//        try {
//            return in.readDouble();
//        } catch (IOException e) {
//            throw new ParquetDecodingException("could not read double", e);
//        }
//    }
//
//    @Override
//    public IValueReference readBytes() {
//        try {
//            return in.readBytes(valueStorage, valueLength);
//        } catch (IOException e) {
//            throw new ParquetDecodingException("could not read bytes", e);
//        }
//    }
//}
