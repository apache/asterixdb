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
import java.nio.ByteBuffer;

import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.column.values.IColumnKeyValueReader;
import org.apache.asterix.column.values.reader.value.AbstractValueReader;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.util.string.UTF8StringUtil;

public final class StringKeyValueReader extends AbstractValueReader implements IColumnKeyValueReader {
    private final IPointable value;
    private ByteBuffer buffer;
    private int startOffset;
    private int tupleCount;

    public StringKeyValueReader() {
        value = new VoidPointable();
    }

    @Override
    public void init(AbstractBytesInputStream in, int tupleCount) throws IOException {
        buffer = in.getBuffer();
        startOffset = buffer.position();
        this.tupleCount = tupleCount;
        value.set(null, 0, 0);
    }

    @Override
    public int reset(int startIndex, int skipCount) {
        getValue(startIndex);
        return 0;
    }

    @Override
    public IValueReference getValue(int index) {
        byte[] bytes = buffer.array();
        int indexOffset = startOffset + index * Integer.BYTES;
        int valueOffset = startOffset + tupleCount * Integer.BYTES + IntegerPointable.getInteger(bytes, indexOffset);
        int valueLength = UTF8StringUtil.getUTFLength(bytes, valueOffset);
        valueLength += UTF8StringUtil.getNumBytesToStoreLength(valueLength);
        value.set(bytes, valueOffset, valueLength);
        return value;
    }

    @Override
    public IValueReference getBytes() {
        return value;
    }

    @Override
    public void nextValue() {
        if (value.getByteArray() == null) {
            getValue(0);
            return;
        }
        int offset = value.getStartOffset() + value.getLength();
        int length = UTF8StringUtil.getUTFLength(buffer.array(), offset);
        length += UTF8StringUtil.getNumBytesToStoreLength(length);
        value.set(buffer.array(), offset, length);
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.STRING;
    }

    @Override
    public int compareTo(AbstractValueReader o) {
        return UTF8StringPointable.compare(getBytes(), o.getBytes());
    }
}
