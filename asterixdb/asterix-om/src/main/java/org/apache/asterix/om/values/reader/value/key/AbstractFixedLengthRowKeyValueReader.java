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
package org.apache.asterix.om.values.reader.value.key;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.om.lazy.metadata.stream.in.AbstractRowBytesInputStream;
import org.apache.asterix.om.values.IRowKeyValueReader;
import org.apache.asterix.om.values.reader.value.AbstractRowValueReader;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;

abstract class AbstractFixedLengthRowKeyValueReader extends AbstractRowValueReader implements IRowKeyValueReader {
    protected final IPointable value;
    private ByteBuffer buffer;
    private int startOffset;

    AbstractFixedLengthRowKeyValueReader() {
        value = new VoidPointable();
    }

    @Override
    public void init(AbstractRowBytesInputStream in, int tupleCount) throws IOException {
        buffer = in.getBuffer();
        startOffset = buffer.position();
        value.set(null, 0, 0);
    }

    @Override
    public void reset(int startIndex, int skipCount) {
        getValue(startIndex);
    }

    @Override
    public IValueReference getValue(int index) {
        int valueLength = getValueLength();
        int offset = startOffset + index * valueLength;
        value.set(buffer.array(), offset, valueLength);
        return value;
    }

    @Override
    public void nextValue() {
        if (value.getByteArray() == null) {
            getValue(0);
            return;
        }
        int valueLength = getValueLength();
        int offset = value.getStartOffset() + valueLength;
        value.set(buffer.array(), offset, valueLength);
    }

    protected abstract int getValueLength();

}
