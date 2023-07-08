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
//import org.apache.asterix.om.api.IRowWriteMultiPageOp;
//import org.apache.asterix.om.types.ATypeTag;
//import org.apache.asterix.om.values.IRowValuesWriter;
//import org.apache.asterix.om.values.IRowValuesWriterFactory;
//import org.apache.commons.lang3.mutable.Mutable;
//
//public class RowValuesWriterFactory implements IRowValuesWriterFactory {
//    private final Mutable<IRowWriteMultiPageOp> multiPageOpRef;
//
//    public RowValuesWriterFactory(Mutable<IRowWriteMultiPageOp> multiPageOpRef) {
//        this.multiPageOpRef = multiPageOpRef;
//    }
//
//    @Override
//    public IRowValuesWriter createValueWriter(ATypeTag typeTag, int columnIndex, int maxLevel, boolean writeAlways,
//            boolean filtered) {
//        switch (typeTag) {
//            case MISSING:
//            case NULL:
//                return new NullMissingRowValuesWriter(columnIndex, maxLevel, writeAlways, filtered);
//            case BOOLEAN:
//                return new BooleanRowValuesWriter(columnIndex, maxLevel, writeAlways, filtered);
//            case BIGINT:
//                return new LongRowValuesWriter(multiPageOpRef, columnIndex, maxLevel, writeAlways, filtered);
//            case DOUBLE:
//                return new DoubleRowValuesWriter(multiPageOpRef, columnIndex, maxLevel, writeAlways, filtered);
//            case STRING:
//                return new StringRowValuesWriter(multiPageOpRef, columnIndex, maxLevel, writeAlways, filtered);
//            case UUID:
//                return new UUIDRowValuesWriter(multiPageOpRef, columnIndex, maxLevel, writeAlways, filtered);
//            default:
//                throw new UnsupportedOperationException(typeTag + " is not supported");
//        }
//    }
//}
