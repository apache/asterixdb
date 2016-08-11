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

import org.apache.hyracks.api.dataflow.value.IRangeMap;

/**
 * The range map stores the field split values in an byte array.
 * The first and last split values for each column represent the min and max values (not actually split values).
 * <br />
 * Example for columns A and B with three split values.
 * {min A, min B, split 1 A, split 1 B, split 2 A, split 2 B, split 3 A, split 3 B, max A, max B}
 */
public class RangeMap implements IRangeMap, Serializable {
    private static final long serialVersionUID = 1L;
    private final int fields;
    private final byte[] bytes;
    private final int[] offsets;

    public RangeMap(int fields, byte[] bytes, int[] offsets) {
        this.fields = fields;
        this.bytes = bytes;
        this.offsets = offsets;
    }

    @Override
    public int getSplitCount() {
        return offsets.length / fields - 2;
    }

    @Override
    public byte[] getByteArray(int columnIndex, int splitIndex) {
        return bytes;
    }

    @Override
    public int getTag(int columnIndex, int splitIndex) {
        return getFieldTag(getFieldIndex(columnIndex, splitIndex + 1));
    }

    @Override
    public int getStartOffset(int columnIndex, int splitIndex) {
        return getFieldStart(getFieldIndex(columnIndex, splitIndex + 1));
    }

    @Override
    public int getLength(int columnIndex, int splitIndex) {
        return getFieldLength(getFieldIndex(columnIndex, splitIndex + 1));
    }

    private int getFieldIndex(int columnIndex, int splitIndex) {
        return columnIndex + splitIndex * fields;
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

    @Override
    public byte[] getMinByteArray(int columnIndex) {
        return bytes;
    }

    @Override
    public int getMinStartOffset(int columnIndex) {
        return getFieldStart(getFieldIndex(columnIndex, getMinIndex()));
    }

    @Override
    public int getMinLength(int columnIndex) {
        return getFieldLength(getFieldIndex(columnIndex, getMinIndex()));
    }

    @Override
    public int getMinTag(int columnIndex) {
        return getFieldTag(getFieldIndex(columnIndex, getMinIndex()));
    }

    @Override
    public byte[] getMaxByteArray(int columnIndex) {
        return bytes;
    }

    @Override
    public int getMaxStartOffset(int columnIndex) {
        return getFieldStart(getFieldIndex(columnIndex, getMaxIndex()));
    }

    @Override
    public int getMaxLength(int columnIndex) {
        return getFieldLength(getFieldIndex(columnIndex, getMaxIndex()));
    }

    @Override
    public int getMaxTag(int columnIndex) {
        return getFieldTag(getFieldIndex(columnIndex, getMaxIndex()));
    }

    private int getMaxIndex() {
        return offsets.length / fields - 1;
    }

    private int getMinIndex() {
        return 0;
    }

}
