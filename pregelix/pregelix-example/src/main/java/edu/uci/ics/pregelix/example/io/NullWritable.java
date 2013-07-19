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

import edu.uci.ics.pregelix.api.io.WritableSizable;

/** Singleton Writable with no data. */
@SuppressWarnings("rawtypes")
public class NullWritable implements WritableComparable, WritableSizable {

    private static final NullWritable THIS = new NullWritable();

    private NullWritable() {
    } // no public ctor

    /** Returns the single instance of this class. */
    public static NullWritable get() {
        return THIS;
    }

    public String toString() {
        return "(null)";
    }

    public int sizeInBytes() {
        return 0;
    }

    public int hashCode() {
        return 0;
    }

    public int compareTo(Object other) {
        if (!(other instanceof NullWritable)) {
            throw new ClassCastException("can't compare " + other.getClass().getName() + " to NullWritable");
        }
        return 0;
    }

    public boolean equals(Object other) {
        return other instanceof NullWritable;
    }

    public void readFields(DataInput in) throws IOException {
    }

    public void write(DataOutput out) throws IOException {
    }

    /** A Comparator &quot;optimized&quot; for NullWritable. */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(NullWritable.class);
        }

        /**
         * Compare the buffers in serialized form.
         */
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            assert 0 == l1;
            assert 0 == l2;
            return 0;
        }
    }

    static { // register this comparator
        WritableComparator.define(NullWritable.class, new Comparator());
    }
}
