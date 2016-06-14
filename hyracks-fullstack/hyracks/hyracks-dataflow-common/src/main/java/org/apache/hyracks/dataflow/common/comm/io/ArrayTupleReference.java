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
package org.apache.hyracks.dataflow.common.comm.io;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * An ArrayTupleReference provides access to a tuple that is not serialized into
 * a frame. It is meant to be reset directly with the field slots and tuple data
 * provided by ArrayTupleBuilder. The purpose is to avoid coping the built tuple
 * into a frame before being able to use it as an ITupleReference.
 *
 * @author alexander.behm
 */
public class ArrayTupleReference implements ITupleReference {
    private int[] fEndOffsets;
    private byte[] tupleData;

    public void reset(int[] fEndOffsets, byte[] tupleData) {
        this.fEndOffsets = fEndOffsets;
        this.tupleData = tupleData;
    }

    @Override
    public int getFieldCount() {
        return fEndOffsets.length;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return tupleData;
    }

    @Override
    public int getFieldStart(int fIdx) {
        return (fIdx == 0) ? 0 : fEndOffsets[fIdx - 1];
    }

    @Override
    public int getFieldLength(int fIdx) {
        return (fIdx == 0) ? fEndOffsets[0] : fEndOffsets[fIdx] - fEndOffsets[fIdx - 1];
    }
}
