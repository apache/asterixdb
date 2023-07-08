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
//import org.apache.asterix.om.bytes.decoder.ParquetRowRunLengthBitPackingHybridDecoder;
//import org.apache.asterix.om.lazy.metadata.stream.in.AbstractRowBytesInputStream;
//import org.apache.asterix.om.lazy.metadata.stream.in.ByteRowBufferInputStream;
//import org.apache.asterix.om.lazy.metadata.stream.in.MultiRowByteBufferInputStream;
//import org.apache.asterix.om.types.ATypeTag;
//import org.apache.asterix.om.utils.RowValuesUtil;
//import org.apache.asterix.om.values.IRowValuesReader;
//import org.apache.asterix.om.values.IRowValuesWriter;
//import org.apache.asterix.om.values.reader.value.AbstractRowValueReader;
//import org.apache.hyracks.api.exceptions.HyracksDataException;
//import org.apache.hyracks.data.std.api.IValueReference;
//import org.apache.parquet.bytes.BytesUtils;
//
//abstract class AbstractRowValuesReader implements IRowValuesReader {
//    protected final AbstractRowValueReader valueReader;
//    protected final int columnIndex;
//    protected final int maxLevel;
//    protected final ParquetRowRunLengthBitPackingHybridDecoder definitionLevels;
//    protected final AbstractRowBytesInputStream valuesStream;
//    protected int level;
//    protected int valueCount;
//    protected int valueIndex;
//
//    private int nullBitMask;
//    private boolean nullLevel;
//    private boolean allMissing;
//
//    AbstractRowValuesReader(AbstractRowValueReader valueReader, int columnIndex, int maxLevel, boolean primaryKey) {
//        this.valueReader = valueReader;
//        this.columnIndex = columnIndex;
//        this.maxLevel = maxLevel;
//        definitionLevels = new ParquetRowRunLengthBitPackingHybridDecoder(RowValuesUtil.getBitWidth(maxLevel));
//        valuesStream = primaryKey ? new ByteRowBufferInputStream() : new MultiRowByteBufferInputStream();
//    }
//
//    final void nextLevel() throws HyracksDataException {
//        if (allMissing) {
//            return;
//        }
//
//        valueIndex++;
//        try {
//            int actualLevel = definitionLevels.readInt();
//            //Check whether the level is for a null value
//            nullLevel = RowValuesUtil.isNull(nullBitMask, actualLevel);
//            //Clear the null bit to allow repeated value readers determine the correct delimiter for null values
//            level = RowValuesUtil.clearNullBit(nullBitMask, actualLevel);
//        } catch (IOException e) {
//            throw HyracksDataException.create(e);
//        }
//    }
//
//    abstract void resetValues();
//
//    @Override
//    public final void reset(AbstractRowBytesInputStream in, int tupleCount) throws HyracksDataException {
//        valueIndex = 0;
//        if (in.available() == 0) {
//            allMissing = true;
//            level = 0;
//            valueCount = tupleCount;
//            return;
//        }
//        allMissing = false;
//        try {
//            nullBitMask = RowValuesUtil.getNullMask(BytesUtils.readZigZagVarInt(in));
//            int defLevelsSize = BytesUtils.readZigZagVarInt(in);
//            valueCount = BytesUtils.readZigZagVarInt(in);
//            definitionLevels.reset(in);
//            valuesStream.resetAt(defLevelsSize, in);
//            int valueLength = BytesUtils.readZigZagVarInt(valuesStream);
//            if (valueLength > 0) {
//                valueReader.init(valuesStream, tupleCount);
//            }
//        } catch (IOException e) {
//            throw HyracksDataException.create(e);
//        }
//        resetValues();
//    }
//
//    @Override
//    public final ATypeTag getTypeTag() {
//        return valueReader.getTypeTag();
//    }
//
//    @Override
//    public final int getColumnIndex() {
//        return columnIndex;
//    }
//
//    @Override
//    public int getLevel() {
//        return level;
//    }
//
//    @Override
//    public final boolean isMissing() {
//        return !isDelimiter() && level < maxLevel;
//    }
//
//    @Override
//    public final boolean isNull() {
//        return nullLevel;
//    }
//
//    @Override
//    public final boolean isValue() {
//        return !isNull() && level == maxLevel;
//    }
//
//    @Override
//    public final long getLong() {
//        return valueReader.getLong();
//    }
//
//    @Override
//    public final double getDouble() {
//        return valueReader.getDouble();
//    }
//
//    @Override
//    public final boolean getBoolean() {
//        return valueReader.getBoolean();
//    }
//
//    @Override
//    public final IValueReference getBytes() {
//        return valueReader.getBytes();
//    }
//
//    @Override
//    public final int compareTo(IRowValuesReader o) {
//        return valueReader.compareTo(((AbstractRowValuesReader) o).valueReader);
//    }
//
//    @Override
//    public final void write(IRowValuesWriter writer, int count) throws HyracksDataException {
//        for (int i = 0; i < count; i++) {
//            write(writer, true);
//        }
//    }
//
//    @Override
//    public void skip(int count) throws HyracksDataException {
//        for (int i = 0; i < count; i++) {
//            next();
//        }
//    }
//}
