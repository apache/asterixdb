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

package org.apache.hyracks.storage.am.vector;

import java.util.Arrays;

import org.apache.hyracks.storage.am.common.CheckTuple;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class VectorCheckTuple extends CheckTuple {

    /**
     * Wrapper for float arrays to make them Comparable
     */
    public static class FloatArrayWrapper implements Comparable<FloatArrayWrapper> {
        private final float[] array;

        public FloatArrayWrapper(float[] array) {
            this.array = array;
        }

        public float[] getArray() {
            return array;
        }

        @Override
        public int compareTo(FloatArrayWrapper other) {
            return Arrays.compare(this.array, other.array);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null || getClass() != obj.getClass())
                return false;
            FloatArrayWrapper other = (FloatArrayWrapper) obj;
            return Arrays.equals(array, other.array);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(array);
        }

        @Override
        public String toString() {
            return Arrays.toString(array);
        }
    }

    /**
     * Wrapper for double arrays to make them Comparable
     */
    public static class DoubleArrayWrapper implements Comparable<DoubleArrayWrapper> {
        private final double[] array;

        public DoubleArrayWrapper(double[] array) {
            this.array = array;
        }

        public double[] getArray() {
            return array;
        }

        @Override
        public int compareTo(DoubleArrayWrapper other) {
            return Arrays.compare(this.array, other.array);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null || getClass() != obj.getClass())
                return false;
            DoubleArrayWrapper other = (DoubleArrayWrapper) obj;
            return Arrays.equals(array, other.array);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(array);
        }

        @Override
        public String toString() {
            return Arrays.toString(array);
        }
    }

    public VectorCheckTuple(int numFields, int numKeys) {
        super(numFields, numKeys);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof VectorCheckTuple) {
            VectorCheckTuple other = (VectorCheckTuple) o;
            for (int i = 0; i < fields.length; i++) {
                if ((fields[i] instanceof FloatArrayWrapper && other.getField(i) instanceof FloatArrayWrapper)
                        || (fields[i] instanceof DoubleArrayWrapper
                                && other.getField(i) instanceof DoubleArrayWrapper)) {
                    // Special handling for array wrappers (vectors)
                    if (!fields[i].equals(other.getField(i))) {
                        return false;
                    }
                } else {
                    int cmp = fields[i].compareTo(other.getField(i));
                    if (cmp != 0) {
                        return false;
                    }
                }
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < fields.length; i++) {
            if (i > 0)
                sb.append(", ");
            if (fields[i] instanceof FloatArrayWrapper) {
                sb.append(((FloatArrayWrapper) fields[i]).toString());
            } else if (fields[i] instanceof DoubleArrayWrapper) {
                sb.append(((DoubleArrayWrapper) fields[i]).toString());
            } else {
                sb.append(fields[i]);
            }
        }
        sb.append("]");
        return sb.toString();
    }
}
