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
//package org.apache.asterix.om.query;
//
//import org.apache.asterix.column.assembler.AbstractPrimitiveValueAssembler;
//import org.apache.asterix.column.assembler.AssemblerBuilderVisitor;
//import org.apache.asterix.column.assembler.AssemblerState;
//import org.apache.asterix.column.assembler.ObjectValueAssembler;
//import org.apache.asterix.column.assembler.value.IValueGetterFactory;
//import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
//import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
//import org.apache.asterix.column.values.IColumnValuesReaderFactory;
//import org.apache.asterix.om.types.ARecordType;
//import org.apache.hyracks.api.exceptions.HyracksDataException;
//import org.apache.hyracks.data.std.api.IValueReference;
//
//public final class ColumnAssembler {
//    private final AbstractPrimitiveValueAssembler[] assemblers;
//    private final ObjectValueAssembler rootAssembler;
//    private final AssemblerState state;
//    private int numberOfTuples;
//    private int tupleIndex;
//
//    public ColumnAssembler(AbstractSchemaNode node, ARecordType declaredType, QueryColumnMetadata columnMetadata,
//            IColumnValuesReaderFactory readerFactory, IValueGetterFactory valueGetterFactory)
//            throws HyracksDataException {
//        AssemblerBuilderVisitor builderVisitor =
//                new AssemblerBuilderVisitor(columnMetadata, readerFactory, valueGetterFactory);
//        assemblers = builderVisitor.createValueAssemblers(node, declaredType);
//        rootAssembler = (ObjectValueAssembler) builderVisitor.getRootAssembler();
//        state = new AssemblerState();
//    }
//
//    public void reset(int numberOfTuples) {
//        this.numberOfTuples = numberOfTuples;
//        tupleIndex = 0;
//    }
//
//    public void resetColumn(AbstractBytesInputStream stream, int ordinal) throws HyracksDataException {
//        assemblers[ordinal].reset(stream, numberOfTuples);
//    }
//
//    public int getColumnIndex(int ordinal) {
//        return assemblers[ordinal].getColumnIndex();
//    }
//
//    public boolean hasNext() {
//        return tupleIndex < numberOfTuples;
//    }
//
//    public IValueReference nextValue() throws HyracksDataException {
//        rootAssembler.start();
//        if (tupleIndex == numberOfTuples) {
//            rootAssembler.end();
//            //return empty record
//            return rootAssembler.getValue();
//        }
//
//        int index = 0;
//        while (index < assemblers.length) {
//            AbstractPrimitiveValueAssembler assembler = assemblers[index];
//            int groupIndex = assembler.next(state);
//            if (groupIndex != AbstractPrimitiveValueAssembler.NEXT_ASSEMBLER) {
//                index = groupIndex;
//            } else {
//                index++;
//            }
//        }
//
//        tupleIndex++;
//        rootAssembler.end();
//        return rootAssembler.getValue();
//    }
//
//    public int getNumberOfColumns() {
//        return assemblers.length;
//    }
//
//    public int skip(int count) throws HyracksDataException {
//        tupleIndex += count;
//        for (int i = 0; i < assemblers.length; i++) {
//            assemblers[i].skip(count);
//        }
//        return tupleIndex;
//    }
//
//    public void setAt(int index) throws HyracksDataException {
//        skip(index - tupleIndex);
//    }
//}
