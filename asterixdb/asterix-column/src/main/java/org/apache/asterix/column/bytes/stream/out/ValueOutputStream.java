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
package org.apache.asterix.column.bytes.stream.out;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;

public final class ValueOutputStream extends OutputStream {
    private final OutputStream out;
    private final byte[] writeBuffer;

    public ValueOutputStream(OutputStream out) {
        this.out = out;
        writeBuffer = new byte[8];
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    public void writeInt(int value) throws IOException {
        IntegerPointable.setInteger(writeBuffer, 0, value);
        out.write(writeBuffer, 0, Integer.BYTES);
    }

    public void writeLong(long value) throws IOException {
        LongPointable.setLong(writeBuffer, 0, value);
        out.write(writeBuffer, 0, Long.BYTES);
    }

    public void writeFloat(float value) throws IOException {
        FloatPointable.setFloat(writeBuffer, 0, value);
        out.write(writeBuffer, 0, Float.BYTES);
    }

    public void writeDouble(double value) throws IOException {
        DoublePointable.setDouble(writeBuffer, 0, value);
        out.write(writeBuffer, 0, Double.BYTES);
    }
}
