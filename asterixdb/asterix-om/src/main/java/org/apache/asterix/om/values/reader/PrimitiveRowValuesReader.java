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
//package org.apache.asterix.om.values.reader;
//
//import java.io.IOException;
//
//import org.apache.asterix.om.values.IRowKeyValueReader;
//import org.apache.asterix.om.values.IRowValuesWriter;
//import org.apache.asterix.om.values.reader.value.AbstractRowValueReader;
//import org.apache.hyracks.api.exceptions.HyracksDataException;
//import org.apache.hyracks.data.std.api.IValueReference;
//
///**
// * Reader for a non-repeated primitive value
// */
//public final class PrimitiveRowValuesReader extends AbstractRowValuesReader implements IRowKeyValueReader {
//    /**
//     * A primary key value is always present. Anti-matter can be determined by checking whether the definition level
//     * indicates that the tuple's values are missing (i.e., by calling {@link #isMissing()}).
//     */
//    private final boolean primaryKey;
//
//    public PrimitiveRowValuesReader(AbstractRowValueReader reader, int columnIndex, int maxLevel, boolean primaryKey) {
//        super(reader, columnIndex, maxLevel, primaryKey);
//        this.primaryKey = primaryKey;
//    }
//
//    @Override
//    public void resetValues() {
//        //NoOp
//    }
//
//    @Override
//    public boolean next() throws HyracksDataException {
//        if (valueIndex == valueCount) {
//            return false;
//        }
//
//        try {
//            nextLevel();
//            if (primaryKey || level == maxLevel) {
//                valueReader.nextValue();
//            }
//        } catch (IOException e) {
//            throw HyracksDataException.create(e);
//        }
//        return true;
//    }
//
//    @Override
//    public boolean isRepeated() {
//        return false;
//    }
//
//    @Override
//    public boolean isDelimiter() {
//        return false;
//    }
//
//    @Override
//    public boolean isLastDelimiter() {
//        return false;
//    }
//
//    @Override
//    public boolean isRepeatedValue() {
//        return false;
//    }
//
//    @Override
//    public int getNumberOfDelimiters() {
//        return 0;
//    }
//
//    @Override
//    public int getDelimiterIndex() {
//        throw new IllegalStateException("Not a repeated reader");
//    }
//
//    @Override
//    public void write(IRowValuesWriter writer, boolean callNext) throws HyracksDataException {
//        if (callNext && !next()) {
//            throw new IllegalStateException("No more values");
//        }
//
//        writer.writeLevel(level);
//        if (primaryKey || isValue()) {
//            try {
//                writer.writeValue(this);
//            } catch (IOException e) {
//                throw HyracksDataException.create(e);
//            }
//        }
//    }
//
//    @Override
//    public IValueReference getValue(int index) {
//        return ((IRowKeyValueReader) valueReader).getValue(index);
//    }
//
//    @Override
//    public void reset(int startIndex, int skipCount) throws HyracksDataException {
//        ((IRowKeyValueReader) valueReader).reset(startIndex, skipCount);
//        nextLevel();
//        for (int i = 1; i < skipCount; i++) {
//            nextLevel();
//        }
//    }
//}
