/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.pregelix.example.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.pregelix.api.io.Pointable;
import edu.uci.ics.pregelix.api.io.WritableSizable;
import edu.uci.ics.pregelix.example.utils.SerDeUtils;

/**
 * Writable for Double values.
 */
public class DoubleWritable extends org.apache.hadoop.io.DoubleWritable implements WritableSizable, Pointable {

    private byte[] data = new byte[8];

    public DoubleWritable(double value) {
        set(value);
    }

    public DoubleWritable() {
        set(0.0);
    }

    public void set(double v) {
        super.set(v);
        SerDeUtils.writeLong(Double.doubleToLongBits(v), data, 0);
    }

    public int sizeInBytes() {
        return 8;
    }

    @Override
    public byte[] getByteArray() {
        return data;
    }

    @Override
    public int getStartOffset() {
        return 0;
    }

    @Override
    public int getLength() {
        return 8;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        SerDeUtils.writeLong(Double.doubleToLongBits(get()), data, 0);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.write(data);
    }

    @Override
    public int set(byte[] bytes, int offset) {
        super.set(Double.longBitsToDouble(SerDeUtils.readLong(bytes, offset)));
        System.arraycopy(bytes, offset, data, 0, 8);
        return 8;
    }

}
