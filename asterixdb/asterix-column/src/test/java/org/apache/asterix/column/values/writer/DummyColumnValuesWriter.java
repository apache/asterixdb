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
package org.apache.asterix.column.values.writer;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.column.util.ColumnValuesUtil;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class DummyColumnValuesWriter implements IColumnValuesWriter {
    private final RunLengthIntArray definitionLevels;
    private final List<IValueReference> values;
    private final int level;
    private final int nullMask;

    DummyColumnValuesWriter(int level) {
        definitionLevels = new RunLengthIntArray();
        values = new ArrayList<>();
        this.level = level;
        nullMask = ColumnValuesUtil.getNullMask(level);
    }

    public String getDefinitionLevelsString() {
        return definitionLevels.toString();
    }

    public RunLengthIntArray getDefinitionLevels() {
        return definitionLevels;
    }

    public List<IValueReference> getValues() {
        return values;
    }

    @Override
    public void reset() {

    }

    @Override
    public int getColumnIndex() {
        return 0;
    }

    @Override
    public void writeValue(ATypeTag tag, IValueReference value) throws HyracksDataException {
        definitionLevels.add(level);
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage(value.getLength());
        storage.append(value);
        values.add(storage);
    }

    @Override
    public void writeLevel(int level) throws HyracksDataException {
        definitionLevels.add(level);
    }

    @Override
    public void writeLevels(int level, int count) throws HyracksDataException {
        for (int i = 0; i < count; i++) {
            definitionLevels.add(level);
        }
    }

    @Override
    public void writeNull(int level) throws HyracksDataException {
        definitionLevels.add(level | nullMask);
    }

    @Override
    public void writeValue(IColumnValuesReader reader) throws HyracksDataException {
        //NoOp
    }

    @Override
    public void writeAntiMatter(ATypeTag tag, IValueReference value) throws HyracksDataException {
        //NoOp
    }

    @Override
    public int getEstimatedSize() {
        return 0;
    }

    @Override
    public int getEstimatedSize(int length) {
        return length;
    }

    @Override
    public int getAllocatedSpace() {
        return 0;
    }

    @Override
    public int getCount() {
        return 0;
    }

    @Override
    public long getNormalizedMinValue() {
        return 0;
    }

    @Override
    public long getNormalizedMaxValue() {
        return 0;
    }

    @Override
    public void flush(OutputStream out) throws HyracksDataException {

    }

    @Override
    public void close() {

    }

    @Override
    public void serialize(DataOutput output) throws IOException {

    }

    @Override
    public RunLengthIntArray getDefinitionLevelsIntArray() {
        return definitionLevels;
    }
}
