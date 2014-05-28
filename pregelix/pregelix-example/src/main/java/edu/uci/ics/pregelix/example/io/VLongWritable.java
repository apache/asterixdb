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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import edu.uci.ics.pregelix.api.io.Pointable;
import edu.uci.ics.pregelix.api.io.WritableSizable;
import edu.uci.ics.pregelix.example.utils.SerDeUtils;

/**
 * A WritableComparable for longs in a variable-length format. Such values take
 * between one and nine bytes. Smaller values take fewer bytes.
 * 
 * @see org.apache.hadoop.io.WritableUtils#readVLong(DataInput)
 */
@SuppressWarnings("rawtypes")
public class VLongWritable extends org.apache.hadoop.io.VLongWritable implements WritableComparable, WritableSizable,
        Pointable {

    private byte[] data = new byte[10];
    private int numBytes = -1;

    public VLongWritable() {
        set(0);
    }

    public VLongWritable(long value) {
        set(value);
    }

    @Override
    public void set(long value) {
        super.set(value);
        reset();
    }

    public int sizeInBytes() {
        return numBytes;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        numBytes = 0;
        byte firstByte = input.readByte();
        data[numBytes++] = firstByte;
        int len = WritableUtils.decodeVIntSize(firstByte);
        if (len == 1) {
            super.set(firstByte);
            return;
        }
        long i = 0;
        input.readFully(data, numBytes, len - 1);
        numBytes += len - 1;
        for (int idx = 1; idx < len; idx++) {
            byte b = data[idx];
            i = i << 8;
            i = i | (b & 0xFF);
        }
        super.set((WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i));
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.write(data, 0, numBytes);
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
        return numBytes;
    }

    @Override
    public int set(byte[] bytes, int offset) {
        int position = offset;
        numBytes = 0;
        byte firstByte = bytes[position++];
        data[numBytes++] = firstByte;
        int len = WritableUtils.decodeVIntSize(firstByte);
        if (len == 1) {
            super.set(firstByte);
            return numBytes;
        }
        long i = 0;
        System.arraycopy(bytes, position, data, numBytes, len - 1);
        numBytes += len - 1;
        for (int idx = 1; idx < len; idx++) {
            byte b = data[idx];
            i = i << 8;
            i = i | (b & 0xFF);
        }
        super.set((WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i));
        return numBytes;
    }

    private void reset() {
        numBytes = 0;
        long value = get();
        if (value >= -112 && value <= 127) {
            data[numBytes++] = (byte) value;
            return;
        }

        int len = -112;
        if (value < 0) {
            value ^= -1L; // take one's complement'
            len = -120;
        }

        long tmp = value;
        while (tmp != 0) {
            tmp = tmp >> 8;
            len--;
        }

        data[numBytes++] = (byte) len;
        len = (len < -120) ? -(len + 120) : -(len + 112);
        for (int idx = len; idx != 0; idx--) {
            int shiftbits = (idx - 1) * 8;
            long mask = 0xFFL << shiftbits;
            data[numBytes++] = (byte) ((value & mask) >> shiftbits);
        }
    }

    /** A Comparator optimized for LongWritable. */
    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(VLongWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                long thisValue = SerDeUtils.readVLong(b1, s1, l1);
                long thatValue = SerDeUtils.readVLong(b2, s2, l2);
                return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /** A decreasing Comparator optimized for LongWritable. */
    public static class DecreasingComparator extends Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    static { // register default comparator
        WritableComparator.define(VLongWritable.class, new Comparator());
    }

}
