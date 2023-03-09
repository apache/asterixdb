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

import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesReaderFactory;
import org.apache.asterix.column.values.reader.value.AbstractValueReader;
import org.apache.asterix.column.values.reader.value.BooleanValueReader;
import org.apache.asterix.column.values.reader.value.DoubleValueReader;
import org.apache.asterix.column.values.reader.value.LongValueReader;
import org.apache.asterix.column.values.reader.value.NoOpValueReader;
import org.apache.asterix.column.values.reader.value.StringValueReader;
import org.apache.asterix.column.values.reader.value.UUIDValueReader;
import org.apache.asterix.om.types.ATypeTag;

public class ColumnValueReaderFactory implements IColumnValuesReaderFactory {
    @Override
    public IColumnValuesReader createValueReader(ATypeTag typeTag, int columnIndex, int maxLevel, boolean primaryKey) {
        return new PrimitiveColumnValuesReader(createReader(typeTag), columnIndex, maxLevel, primaryKey);
    }

    @Override
    public IColumnValuesReader createValueReader(ATypeTag typeTag, int columnIndex, int maxLevel, int[] delimiters) {
        return new RepeatedPrimitiveColumnValuesReader(createReader(typeTag), columnIndex, maxLevel, delimiters);
    }

    @Override
    public IColumnValuesReader createValueReader(DataInput input) throws IOException {
        ATypeTag typeTag = ATypeTag.VALUE_TYPE_MAPPING[input.readByte()];
        int columnIndex = input.readInt();
        int maxLevel = input.readInt();
        boolean primaryKey = input.readBoolean();
        boolean collection = input.readBoolean();
        if (collection) {
            int[] delimiters = new int[input.readInt()];
            for (int i = 0; i < delimiters.length; i++) {
                delimiters[i] = input.readInt();
            }
            return createValueReader(typeTag, columnIndex, maxLevel, delimiters);
        }
        return createValueReader(typeTag, columnIndex, maxLevel, primaryKey);
    }

    private AbstractValueReader createReader(ATypeTag typeTag) {
        switch (typeTag) {
            case MISSING:
            case NULL:
                return NoOpValueReader.INSTANCE;
            case BOOLEAN:
                return new BooleanValueReader();
            case BIGINT:
                return new LongValueReader();
            case DOUBLE:
                return new DoubleValueReader();
            case STRING:
                return new StringValueReader();
            case UUID:
                return new UUIDValueReader();
            default:
                throw new UnsupportedOperationException(typeTag + " is not supported");
        }
    }
}
