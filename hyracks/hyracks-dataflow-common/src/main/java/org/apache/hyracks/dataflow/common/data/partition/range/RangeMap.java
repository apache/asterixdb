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

import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;

/**
 * The range map stores the field split values in an byte array.
 * The first split value for each field followed by the second split value for each field, etc.
 */
public class RangeMap implements IRangeMap, Serializable {
    private final int fields;
    private final byte[] bytes;
    private final int[] offsets;

    public RangeMap(int fields, byte[] bytes, int[] offsets) {
        this.fields = fields;
        this.bytes = bytes;
        this.offsets = offsets;
    }

    @Override
    public IPointable getFieldSplit(int columnIndex, int splitIndex) {
        IPointable p = VoidPointable.FACTORY.createPointable();
        int index = getFieldIndex(columnIndex, splitIndex);
        p.set(bytes, getFieldStart(index), getFieldLength(index));
        return p;
    }

    @Override
    public int getSplitCount() {
        return offsets.length / fields;
    }

    @Override
    public byte[] getByteArray(int columnIndex, int splitIndex) {
        return bytes;
    }

    @Override
    public int getTag(int columnIndex, int splitIndex) {
        return getFieldTag(getFieldIndex(columnIndex, splitIndex));
    }

    @Override
    public int getStartOffset(int columnIndex, int splitIndex) {
        return getFieldStart(getFieldIndex(columnIndex, splitIndex));
    }

    @Override
    public int getLength(int columnIndex, int splitIndex) {
        return getFieldLength(getFieldIndex(columnIndex, splitIndex));
    }

    private int getFieldIndex(int columnIndex, int splitIndex) {
        return splitIndex * fields + columnIndex;
    }

    private int getFieldTag(int index) {
        return bytes[getFieldStart(index)];
    }

    private int getFieldStart(int index) {
        int start = 0;
        if (index != 0) {
            start = offsets[index - 1];
        }
        return start;
    }

    private int getFieldLength(int index) {
        int length = offsets[index];
        if (index != 0) {
            length -= offsets[index - 1];
        }
        return length;
    }

}