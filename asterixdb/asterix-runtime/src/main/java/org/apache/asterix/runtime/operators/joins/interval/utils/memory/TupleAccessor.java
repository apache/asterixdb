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
package org.apache.asterix.runtime.operators.joins.interval.utils.memory;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class TupleAccessor extends FrameTupleAccessor implements ITupleAccessor {
    public static final int UNSET = -2;
    public static final int INITIALIZED = -1;
    private int tupleId = UNSET;

    public TupleAccessor(RecordDescriptor recordDescriptor) {
        super(recordDescriptor);
    }

    @Override
    public void reset(ByteBuffer buffer) {
        reset(buffer, 0, buffer.limit());
        tupleId = INITIALIZED;
    }

    public void reset(TuplePointer tp) {
        throw new IllegalAccessError("Should never call this reset");
    }

    @Override
    public int getTupleStartOffset() {
        return getTupleStartOffset(tupleId);
    }

    @Override
    public int getTupleEndOffset() {
        return getTupleStartOffset(tupleId);
    }

    @Override
    public int getTupleLength() {
        return getTupleLength(tupleId);
    }

    @Override
    public int getAbsFieldStartOffset(int fieldId) {
        return getAbsoluteFieldStartOffset(tupleId, fieldId);
    }

    @Override
    public int getFieldLength(int fieldId) {
        return getFieldLength(tupleId, fieldId);
    }

    @Override
    public int getFieldEndOffset(int fieldId) {
        return getFieldEndOffset(tupleId, fieldId);
    }

    @Override
    public int getFieldStartOffset(int fieldId) {
        return getFieldStartOffset(tupleId, fieldId);
    }

    @Override
    public int getTupleId() {
        return tupleId;
    }

    @Override
    public void getTuplePointer(TuplePointer tp) {
        tp.reset(INITIALIZED, tupleId);
    }

    @Override
    public void setTupleId(int tupleId) {
        this.tupleId = tupleId;
    }

    @Override
    public void reset() {
        tupleId = INITIALIZED;
    }

    @Override
    public boolean hasNext() {
        if (tupleId == UNSET) {
            return false;
        }
        return tupleId + 1 < getTupleCount();
    }

    @Override
    public void next() {
        ++tupleId;
    }

    @Override
    public boolean exists() {
        return INITIALIZED < tupleId && tupleId < getTupleCount();
    }
}
