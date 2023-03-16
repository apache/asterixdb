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
package org.apache.asterix.column.tuple;

import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class AssembledTupleReference implements ITupleReference {
    private final int fieldCount;
    private final int[] offsets;
    private final int[] lengths;
    private byte[] data;

    public AssembledTupleReference(int fieldCount) {
        this.fieldCount = fieldCount;
        offsets = new int[fieldCount];
        lengths = new int[fieldCount];
    }

    public ITupleReference reset(ArrayTupleBuilder tb) {
        data = tb.getByteArray();
        int[] fieldEndOffsets = tb.getFieldEndOffsets();

        int j = fieldEndOffsets.length - 1;
        for (int i = fieldCount - 1; i >= 0; i--) {
            offsets[i] = j == 0 ? 0 : fieldEndOffsets[j - 1];
            lengths[i] = fieldEndOffsets[j] - offsets[i];
            j--;
        }
        return this;
    }

    @Override
    public int getFieldCount() {
        return fieldCount;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return data;
    }

    @Override
    public int getFieldStart(int fIdx) {
        return offsets[fIdx];
    }

    @Override
    public int getFieldLength(int fIdx) {
        return lengths[fIdx];
    }
}
