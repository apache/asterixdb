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

package edu.uci.ics.pregelix.example.lib.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import edu.uci.ics.pregelix.api.io.WritableSizable;
import edu.uci.ics.pregelix.api.util.SerDeUtils;

/**
 * A WritableComparable for longs in a variable-length format. Such values take
 * between one and nine bytes. Smaller values take fewer bytes.
 * 
 * @see org.apache.hadoop.io.WritableUtils#readVLong(DataInput)
 */
@SuppressWarnings("rawtypes")
public class VLongWritable implements WritableComparable, WritableSizable {
    private static long ONE_BYTE_MAX = 2 ^ 7 - 1;
    private static long TWO_BYTE_MAX = 2 ^ 14 - 1;
    private static long THREE_BYTE_MAX = 2 ^ 21 - 1;
    private static long FOUR_BYTE_MAX = 2 ^ 28 - 1;
    private static long FIVE_BYTE_MAX = 2 ^ 35 - 1;;
    private static long SIX_BYTE_MAX = 2 ^ 42 - 1;;
    private static long SEVEN_BYTE_MAX = 2 ^ 49 - 1;;
    private static long EIGHT_BYTE_MAX = 2 ^ 54 - 1;;

    private long value;

    public VLongWritable() {
    }

    public VLongWritable(long value) {
        set(value);
    }

    public int sizeInBytes() {
        if (value >= 0 && value <= ONE_BYTE_MAX) {
            return 1;
        } else if (value > ONE_BYTE_MAX && value <= TWO_BYTE_MAX) {
            return 2;
        } else if (value > TWO_BYTE_MAX && value <= THREE_BYTE_MAX) {
            return 3;
        } else if (value > THREE_BYTE_MAX && value <= FOUR_BYTE_MAX) {
            return 4;
        } else if (value > FOUR_BYTE_MAX && value <= FIVE_BYTE_MAX) {
            return 5;
        } else if (value > FIVE_BYTE_MAX && value <= SIX_BYTE_MAX) {
            return 6;
        } else if (value > SIX_BYTE_MAX && value <= SEVEN_BYTE_MAX) {
            return 7;
        } else if (value > SEVEN_BYTE_MAX && value <= EIGHT_BYTE_MAX) {
            return 8;
        } else {
            return 9;
        }
    }

    /** Set the value of this LongWritable. */
    public void set(long value) {
        this.value = value;
    }

    /** Return the value of this LongWritable. */
    public long get() {
        return value;
    }

    public void readFields(DataInput in) throws IOException {
        value = SerDeUtils.readVLong(in);
    }

    public void write(DataOutput out) throws IOException {
        SerDeUtils.writeVLong(out, value);
    }

    /** Returns true iff <code>o</code> is a VLongWritable with the same value. */
    public boolean equals(Object o) {
        if (!(o instanceof VLongWritable))
            return false;
        VLongWritable other = (VLongWritable) o;
        return this.value == other.value;
    }

    public int hashCode() {
        return (int) value;
    }

    /** Compares two VLongWritables. */
    public int compareTo(Object o) {
        long thisValue = this.value;
        long thatValue = ((VLongWritable) o).value;
        return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }

    public String toString() {
        return Long.toString(value);
    }

    /** A Comparator optimized for LongWritable. */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(VLongWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            long thisValue = SerDeUtils.readVLong(b1, s1, l1);
            long thatValue = SerDeUtils.readVLong(b2, s2, l2);
            return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
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
