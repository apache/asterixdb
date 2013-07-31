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
import org.apache.hadoop.io.WritableUtils;

import edu.uci.ics.pregelix.api.io.WritableSizable;

/**
 * A WritableComparable for integer values stored in variable-length format.
 * Such values take between one and five bytes. Smaller values take fewer bytes.
 * 
 * @see org.apache.hadoop.io.WritableUtils#readVInt(DataInput)
 */
@SuppressWarnings("rawtypes")
public class VIntWritable implements WritableComparable, WritableSizable {
    private int value;

    public VIntWritable() {
    }

    public VIntWritable(int value) {
        set(value);
    }

    public int sizeInBytes() {
        return 5;
    }

    /** Set the value of this VIntWritable. */
    public void set(int value) {
        this.value = value;
    }

    /** Return the value of this VIntWritable. */
    public int get() {
        return value;
    }

    public void readFields(DataInput in) throws IOException {
        value = WritableUtils.readVInt(in);
    }

    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, value);
    }

    /** Returns true iff <code>o</code> is a VIntWritable with the same value. */
    public boolean equals(Object o) {
        if (!(o instanceof VIntWritable))
            return false;
        VIntWritable other = (VIntWritable) o;
        return this.value == other.value;
    }

    public int hashCode() {
        return value;
    }

    /** Compares two VIntWritables. */
    public int compareTo(Object o) {
        int thisValue = this.value;
        int thatValue = ((VIntWritable) o).value;
        return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }

    public String toString() {
        return Integer.toString(value);
    }

}
