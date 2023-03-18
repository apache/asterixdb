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

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesReaderFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.api.IValueReference;

public class DummyColumnValuesReaderFactory implements IColumnValuesReaderFactory {
    private final List<RunLengthIntArray> defLevels;
    private final List<List<IValueReference>> values;

    public DummyColumnValuesReaderFactory(List<RunLengthIntArray> defLevels, List<List<IValueReference>> values) {
        this.defLevels = defLevels;
        this.values = values;
    }

    @Override
    public IColumnValuesReader createValueReader(ATypeTag typeTag, int columnIndex, int maxLevel, boolean primaryKey) {
        RunLengthIntArray columnDefLevels = defLevels.get(columnIndex);
        List<IValueReference> columnValues = values.get(columnIndex);
        return new DummyPrimitiveColumnValueReader(typeTag, columnDefLevels, columnValues, columnIndex, maxLevel);
    }

    @Override
    public IColumnValuesReader createValueReader(ATypeTag typeTag, int columnIndex, int maxLevel, int[] delimiters) {
        RunLengthIntArray columnDefLevels = defLevels.get(columnIndex);
        List<IValueReference> columnValues = values.get(columnIndex);
        return new DummyRepeatedPrimitiveColumnValueReader(typeTag, columnDefLevels, columnValues, columnIndex,
                maxLevel, delimiters);
    }

    @Override
    public IColumnValuesReader createValueReader(DataInput input) throws IOException {
        return null;
    }
}
