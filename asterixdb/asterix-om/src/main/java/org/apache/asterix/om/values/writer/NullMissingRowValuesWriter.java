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
//import org.apache.asterix.om.types.ATypeTag;
//import org.apache.asterix.om.utils.RunRowLengthIntArray;
//import org.apache.asterix.om.values.IRowValuesReader;
//import org.apache.asterix.om.values.writer.filters.AbstractRowFilterWriter;
//import org.apache.asterix.om.values.writer.filters.NoOpRowFilterWriter;
//import org.apache.hyracks.api.exceptions.HyracksDataException;
//import org.apache.hyracks.data.std.api.IValueReference;
//import org.apache.parquet.bytes.BytesInput;
//
//public class NullMissingRowValuesWriter extends AbstractRowValuesWriter {
//    private static final BytesInput EMPTY = BytesInput.empty(); //TODO : CALVIN_DANI remove parquet dependency
//    private final RunRowLengthIntArray defLevelsIntArray;
//
//    NullMissingRowValuesWriter(int columnIndex, int level, boolean collection, boolean filtered) {
//        super(columnIndex, level, collection, filtered);
//        defLevelsIntArray = new RunRowLengthIntArray();
//    }
//
//    @Override
//    protected void addLevel(int level) throws HyracksDataException {
//        defLevelsIntArray.add(level);
//        super.addLevel(level);
//    }
//
//    @Override
//    public void writeLevels(int level, int count) throws HyracksDataException {
//        defLevelsIntArray.add(level, count);
//        super.writeLevels(level, count);
//    }
//
//    @Override
//    protected ATypeTag getTypeTag() {
//        return ATypeTag.NULL;
//    }
//
//    @Override
//    protected void addValue(ATypeTag tag, IValueReference value) throws IOException {
//        throw new IllegalStateException("Null writer should not add value");
//    }
//
//    @Override
//    protected void addValue(IRowValuesReader reader) throws IOException {
//        throw new IllegalStateException("Null writer should not add value");
//    }
//
//    @Override
//    protected BytesInput getBytes() throws IOException {
//        return EMPTY;
//    }
//
//    @Override
//    protected int getValuesEstimatedSize() {
//        return 0;
//    }
//
//    @Override
//    protected int getValuesAllocatedSize() {
//        return 0;
//    }
//
//    @Override
//    protected AbstractRowFilterWriter createFilter() {
//        return NoOpRowFilterWriter.INSTANCE;
//    }
//
//    @Override
//    protected void resetValues() throws HyracksDataException {
//        defLevelsIntArray.reset();
//    }
//
//    @Override
//    protected void closeValues() {
//        defLevelsIntArray.reset();
//    }
//
//    @Override
//    public RunRowLengthIntArray getDefinitionLevelsIntArray() {
//        return defLevelsIntArray;
//    }
//}
