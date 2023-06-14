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

import java.io.IOException;

import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter;
import org.apache.asterix.column.values.writer.filters.NoOpColumnFilterWriter;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.parquet.bytes.BytesInput;

public class NullMissingColumnValuesWriter extends AbstractColumnValuesWriter {
    private static final BytesInput EMPTY = BytesInput.empty();
    private final RunLengthIntArray defLevelsIntArray;

    NullMissingColumnValuesWriter(int columnIndex, int level, boolean collection, boolean filtered) {
        super(columnIndex, level, collection, filtered);
        defLevelsIntArray = new RunLengthIntArray();
    }

    @Override
    protected void addLevel(int level) throws HyracksDataException {
        defLevelsIntArray.add(level);
        super.addLevel(level);
    }

    @Override
    public void writeLevels(int level, int count) throws HyracksDataException {
        defLevelsIntArray.add(level, count);
        super.writeLevels(level, count);
    }

    @Override
    protected ATypeTag getTypeTag() {
        return ATypeTag.NULL;
    }

    @Override
    protected void addValue(ATypeTag tag, IValueReference value) throws IOException {
        throw new IllegalStateException("Null writer should not add value");
    }

    @Override
    protected void addValue(IColumnValuesReader reader) throws IOException {
        throw new IllegalStateException("Null writer should not add value");
    }

    @Override
    protected BytesInput getBytes() throws IOException {
        return EMPTY;
    }

    @Override
    protected int getValuesEstimatedSize() {
        return 0;
    }

    @Override
    protected int calculateEstimatedSize(int length) {
        return 0;
    }

    @Override
    protected int getValuesAllocatedSize() {
        return 0;
    }

    @Override
    protected AbstractColumnFilterWriter createFilter() {
        return NoOpColumnFilterWriter.INSTANCE;
    }

    @Override
    protected void resetValues() throws HyracksDataException {
        defLevelsIntArray.reset();
    }

    @Override
    protected void closeValues() {
        defLevelsIntArray.reset();
    }

    @Override
    public RunLengthIntArray getDefinitionLevelsIntArray() {
        return defLevelsIntArray;
    }
}
