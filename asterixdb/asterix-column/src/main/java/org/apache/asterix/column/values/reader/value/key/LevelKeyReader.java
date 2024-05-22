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
package org.apache.asterix.column.values.reader.value.key;

import java.io.IOException;

import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.column.values.IColumnKeyValueReader;
import org.apache.asterix.column.values.reader.value.AbstractValueReader;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

/**
 * This singleton is used to ignore the key value and provide a way to read the key's level using
 * {@link org.apache.asterix.column.values.reader.PrimitiveColumnValuesReader}
 *
 * @see org.apache.asterix.column.filter.iterable.evaluator.ColumnIterableFilterEvaluatorFactory
 */
public final class LevelKeyReader extends AbstractValueReader implements IColumnKeyValueReader {
    public static final LevelKeyReader INSTANCE = new LevelKeyReader();

    private LevelKeyReader() {
    }

    @Override
    public int reset(int startIndex, int skipCount) throws HyracksDataException {
        return 0;
    }

    @Override
    public IValueReference getValue(int index) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void init(AbstractBytesInputStream in, int tupleCount) throws IOException {
        // NoOp
    }

    @Override
    public void nextValue() throws HyracksDataException {
        // NoOp
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.MISSING;
    }

    @Override
    public int compareTo(AbstractValueReader o) {
        throw new UnsupportedOperationException(getClass().getName());
    }
}
