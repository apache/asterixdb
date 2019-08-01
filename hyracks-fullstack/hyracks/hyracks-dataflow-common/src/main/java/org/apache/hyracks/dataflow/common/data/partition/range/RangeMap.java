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
package org.apache.hyracks.dataflow.common.data.partition.range;

import java.io.Serializable;
import java.util.Arrays;

/**
 * <pre>
 * The range map stores the fields split values in a byte array.
 * The first split value for each field followed by the second split value for each field, etc. For example:
 *                  split_point_idx0    split_point_idx1    split_point_idx2    split_point_idx3    split_point_idx4
 * in the byte[]:   f0,f1,f2            f0,f1,f2            f0,f1,f2            f0,f1,f2            f0,f1,f2
 * numFields would be = 3
 * we have 5 split points, which gives us 6 partitions:
 *      p1  |       p2      |       p3      |       p4      |       p5      |       p6
 *          sp0             sp1             sp2             sp3             sp4
 * endOffsets.length would be = 15
 * </pre>
 */
public class RangeMap implements Serializable {
    private static final long serialVersionUID = -7523433293419648234L;

    private final int fields;
    private final byte[] bytes;
    private final int[] endOffsets;

    public RangeMap(int numFields, byte[] bytes, int[] endOffsets) {
        this.fields = numFields;
        this.bytes = bytes;
        this.endOffsets = endOffsets;
    }

    public int getSplitCount() {
        return endOffsets.length / fields;
    }

    public byte[] getByteArray() {
        return bytes;
    }

    public int getTag(int fieldIndex, int splitIndex) {
        return getSplitValueTag(getSplitValueIndex(fieldIndex, splitIndex));
    }

    public int getStartOffset(int fieldIndex, int splitIndex) {
        return getSplitValueStart(getSplitValueIndex(fieldIndex, splitIndex));
    }

    public int getLength(int fieldIndex, int splitIndex) {
        return getSplitValueLength(getSplitValueIndex(fieldIndex, splitIndex));
    }

    /** Translates fieldIndex & splitIndex into an index which is used to find information about that split value.
     * The combination of a fieldIndex & splitIndex uniquely identifies a split value of interest.
     * @param fieldIndex the field index within the splitIndex of interest (0 <= fieldIndex < numFields)
     * @param splitIndex starts with 0,1,2,.. etc
     * @return the index of the desired split value that could be used with {@code bytes} & {@code endOffsets}.
     */
    private int getSplitValueIndex(int fieldIndex, int splitIndex) {
        return splitIndex * fields + fieldIndex;
    }

    /**
     * @param splitValueIndex is the combination of the split index + the field index within that split index
     * @return the type tag of a specific field in a specific split point
     */
    private int getSplitValueTag(int splitValueIndex) {
        return bytes[getSplitValueStart(splitValueIndex)];
    }

    /**
     * @param splitValueIndex is the combination of the split index + the field index within that split index
     * @return the location of a split value in the byte array {@code bytes}
     */
    private int getSplitValueStart(int splitValueIndex) {
        int start = 0;
        if (splitValueIndex != 0) {
            start = endOffsets[splitValueIndex - 1];
        }
        return start;
    }

    /**
     * @param splitValueIndex is the combination of the split index + the field index within that split index
     * @return the length of a split value
     */
    private int getSplitValueLength(int splitValueIndex) {
        int length = endOffsets[splitValueIndex];
        if (splitValueIndex != 0) {
            length -= endOffsets[splitValueIndex - 1];
        }
        return length;
    }

    @Override
    public int hashCode() {
        return fields + Arrays.hashCode(bytes) + Arrays.hashCode(endOffsets);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof RangeMap)) {
            return false;
        }
        RangeMap other = (RangeMap) object;
        return fields == other.fields && Arrays.equals(endOffsets, other.endOffsets)
                && Arrays.equals(bytes, other.bytes);
    }

    @Override
    public String toString() {
        return "{SPLIT:" + getSplitCount() + '}';
    }
}
