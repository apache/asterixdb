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
package org.apache.asterix.column.values.reader.value;

import java.io.IOException;

import org.apache.asterix.column.bytes.decoder.ParquetPlainFixedLengthValuesReader;
import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.dataflow.data.nontagged.comparators.AUUIDPartialBinaryComparatorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public final class UUIDValueReader extends AbstractValueReader {
    private final ParquetPlainFixedLengthValuesReader uuidReader;
    private IValueReference nextValue;

    public UUIDValueReader() {
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage(16);
        uuidReader = new ParquetPlainFixedLengthValuesReader(storage);
    }

    @Override
    public void init(AbstractBytesInputStream in, int tupleCount) throws IOException {
        uuidReader.initFromPage(in);
    }

    @Override
    public void nextValue() {
        nextValue = uuidReader.readBytes();
    }

    @Override
    public IValueReference getBytes() {
        return nextValue;
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.UUID;
    }

    @Override
    public int compareTo(AbstractValueReader o) {
        return AUUIDPartialBinaryComparatorFactory.compare(nextValue, o.getBytes());
    }
}
