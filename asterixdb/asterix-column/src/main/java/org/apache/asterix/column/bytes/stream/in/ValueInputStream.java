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
package org.apache.asterix.column.bytes.stream.in;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.parquet.bytes.LittleEndianDataInputStream;

/**
 * Re-implementation of {@link LittleEndianDataInputStream}
 */
public final class ValueInputStream extends InputStream {
    private final byte[] readBuffer;
    private InputStream in;

    public ValueInputStream() {
        readBuffer = new byte[8];
    }

    public void reset(AbstractBytesInputStream in) {
        this.in = in;
    }

    @Override
    public int read() throws IOException {
        return in.read();
    }

    public int readInt() throws IOException {
        readFully(readBuffer, Integer.BYTES);
        return IntegerPointable.getInteger(readBuffer, 0);
    }

    public long readLong() throws IOException {
        readFully(readBuffer, Long.BYTES);
        return LongPointable.getLong(readBuffer, 0);
    }

    public float readFloat() throws IOException {
        readFully(readBuffer, Float.BYTES);
        return FloatPointable.getFloat(readBuffer, 0);
    }

    public double readDouble() throws IOException {
        readFully(readBuffer, Double.BYTES);
        return DoublePointable.getDouble(readBuffer, 0);
    }

    public IValueReference readBytes(IPointable valueStorage, int length) throws IOException {
        readFully(valueStorage.getByteArray(), length);
        return valueStorage;
    }

    public void skipBytes(int n) throws IOException {
        int total = 0;
        int cur;

        while ((total < n) && ((cur = (int) in.skip(n - total)) > 0)) {
            total += cur;
        }
    }

    private void readFully(byte[] bytes, int len) throws IOException {
        if (len < 0) {
            throw new IndexOutOfBoundsException();
        } else {
            int count;
            for (int n = 0; n < len; n += count) {
                count = this.in.read(bytes, n, len - n);
                if (count < 0) {
                    throw new EOFException();
                }
            }

        }
    }
}
