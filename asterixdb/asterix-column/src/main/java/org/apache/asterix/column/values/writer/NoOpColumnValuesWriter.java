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

import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public class NoOpColumnValuesWriter implements IColumnValuesWriter {
    public static NoOpColumnValuesWriter INSTANCE = new NoOpColumnValuesWriter();

    @Override
    public void reset() throws HyracksDataException {

    }

    @Override
    public int getColumnIndex() {
        return 0;
    }

    @Override
    public void writeValue(ATypeTag tag, IValueReference value) throws HyracksDataException {

    }

    @Override
    public void writeAntiMatter(ATypeTag tag, IValueReference value) throws HyracksDataException {

    }

    @Override
    public void writeLevel(int level) throws HyracksDataException {

    }

    @Override
    public void writeLevels(int level, int count) throws HyracksDataException {

    }

    @Override
    public RunLengthIntArray getDefinitionLevelsIntArray() {
        return null;
    }

    @Override
    public void writeNull(int level) throws HyracksDataException {

    }

    @Override
    public void writeValue(IColumnValuesReader reader) throws HyracksDataException {

    }

    @Override
    public int getEstimatedSize() {
        return 0;
    }

    @Override
    public int getEstimatedSize(int length) {
        return 0;
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
}
