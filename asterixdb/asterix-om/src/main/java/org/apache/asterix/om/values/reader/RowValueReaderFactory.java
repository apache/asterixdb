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
//import java.io.DataInput;
//import java.io.IOException;
//
//import org.apache.asterix.om.types.ATypeTag;
//import org.apache.asterix.om.values.IRowValuesReader;
//import org.apache.asterix.om.values.IRowValuesReaderFactory;
//import org.apache.asterix.om.values.reader.value.AbstractRowValueReader;
//import org.apache.asterix.om.values.reader.value.BooleanRowValueReader;
//import org.apache.asterix.om.values.reader.value.DoubleRowValueReader;
//import org.apache.asterix.om.values.reader.value.LongRowValueReader;
//import org.apache.asterix.om.values.reader.value.NoOpRowValueReader;
//import org.apache.asterix.om.values.reader.value.StringRowValueReader;
//import org.apache.asterix.om.values.reader.value.UUIDValueRowReader;
//import org.apache.asterix.om.values.reader.value.key.DoubleKeyRowValueReader;
//import org.apache.asterix.om.values.reader.value.key.LongKeyRowValueReader;
//import org.apache.asterix.om.values.reader.value.key.StringKeyValueRowReader;
//import org.apache.asterix.om.values.reader.value.key.UUIDKeyValueRowReader;
//
//public class RowValueReaderFactory implements IRowValuesReaderFactory {
//    @Override
//    public IRowValuesReader createValueReader(ATypeTag typeTag, int columnIndex, int maxLevel, boolean primaryKey) {
//        return new PrimitiveRowValuesReader(createReader(typeTag, primaryKey), columnIndex, maxLevel, primaryKey);
//    }
//
//    @Override
//    public IRowValuesReader createValueReader(ATypeTag typeTag, int columnIndex, int maxLevel, int[] delimiters) {
//        return new RepeatedPrimitiveRowValuesReader(createReader(typeTag, false), columnIndex, maxLevel, delimiters);
//    }
//
//    @Override
//    public IRowValuesReader createValueReader(DataInput input) throws IOException {
//        ATypeTag typeTag = ATypeTag.VALUE_TYPE_MAPPING[input.readByte()];
//        int columnIndex = input.readInt();
//        int maxLevel = input.readInt();
//        boolean primaryKey = input.readBoolean();
//        boolean collection = input.readBoolean();
//        if (collection) {
//            int[] delimiters = new int[input.readInt()];
//            for (int i = 0; i < delimiters.length; i++) {
//                delimiters[i] = input.readInt();
//            }
//            return createValueReader(typeTag, columnIndex, maxLevel, delimiters);
//        }
//        return createValueReader(typeTag, columnIndex, maxLevel, primaryKey);
//    }
//
//    private AbstractRowValueReader createReader(ATypeTag typeTag, boolean primaryKey) {
//        switch (typeTag) {
//            case MISSING:
//            case NULL:
//                return NoOpRowValueReader.INSTANCE;
//            case BOOLEAN:
//                return new BooleanRowValueReader();
//            case BIGINT:
//                return primaryKey ? new LongKeyRowValueReader() : new LongRowValueReader();
//            case DOUBLE:
//                return primaryKey ? new DoubleKeyRowValueReader() : new DoubleRowValueReader();
//            case STRING:
//                return primaryKey ? new StringKeyValueRowReader() : new StringRowValueReader();
//            case UUID:
//                return primaryKey ? new UUIDKeyValueRowReader() : new UUIDValueRowReader();
//            default:
//                throw new UnsupportedOperationException(typeTag + " is not supported");
//        }
//    }
//}
