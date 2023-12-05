/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.column.values.reader;

import java.util.List;

import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.column.util.ColumnValuesUtil;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class AbstractDummyColumnValuesReader implements IColumnValuesReader {
    private final ATypeTag typeTag;
    protected final int columnIndex;
    protected final int maxLevel;
    protected final RunLengthIntArray defLevels;
    protected final List<IValueReference> values;
    protected int level;
    protected int valueCount;
    protected int valueIndex;
    protected int nonMissingValueIndex;

    //Definition levels
    private int blockIndex;
    private int blockSize;
    private int blockValueIndex;

    private final int nullMask;
    private boolean nullLevel;

    AbstractDummyColumnValuesReader(ATypeTag typeTag, RunLengthIntArray defLevels, List<IValueReference> values,
            int columnIndex, int maxLevel) {
        this.typeTag = typeTag;
        this.columnIndex = columnIndex;
        this.maxLevel = maxLevel;
        this.defLevels = defLevels;
        this.values = values;
        this.valueCount = defLevels.getSize();
        nonMissingValueIndex = -1;
        blockIndex = 0;
        nullMask = ColumnValuesUtil.getNullMask(maxLevel);
        nextBlock();
    }

    protected void nextLevel() {
        if (blockValueIndex >= blockSize) {
            nextBlock();
        }
        blockValueIndex++;
    }

    private void nextBlock() {
        blockValueIndex = 0;
        blockSize = defLevels.getBlockSize(blockIndex);

        int actualLevel = defLevels.getBlockValue(blockIndex++);
        nullLevel = ColumnValuesUtil.isNull(nullMask, actualLevel);
        level = ColumnValuesUtil.clearNullBit(nullMask, actualLevel);
    }

    @Override
    public final void reset(AbstractBytesInputStream in, int numberOfTuples) {
        //noOp
    }

    @Override
    public final ATypeTag getTypeTag() {
        return typeTag;
    }

    @Override
    public final int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public int getLevel() {
        return level;
    }

    @Override
    public final boolean isMissing() {
        return !isDelimiter() && level < maxLevel;
    }

    @Override
    public final boolean isNull() {
        return nullLevel;
    }

    @Override
    public final boolean isValue() {
        return !isNull() && level == maxLevel;
    }

    @Override
    public float getFloat() {
        return -1.0f;
    }

    @Override
    public final long getLong() {
        return -1;
    }

    @Override
    public final double getDouble() {
        return -1.0;
    }

    @Override
    public final boolean getBoolean() {
        return false;
    }

    @Override
    public final IValueReference getBytes() {
        return values.get(nonMissingValueIndex);
    }

    @Override
    public final int compareTo(IColumnValuesReader o) {
        return 0;
    }

    @Override
    public void write(IColumnValuesWriter writer, boolean callNext) throws HyracksDataException {
        //NoOp
    }

    @Override
    public void write(IColumnValuesWriter writer, int count) throws HyracksDataException {
        //NoOp
    }

    @Override
    public final void skip(int count) throws HyracksDataException {
        for (int i = 0; i < count; i++) {
            next();
        }
    }

    protected void appendCommon(ObjectNode node) {
        node.put("columnIndex", columnIndex);
        node.put("valueIndex", valueIndex);
        node.put("valueCount", valueCount);
        node.put("level", level);
        node.put("maxLevel", maxLevel);
    }
}
