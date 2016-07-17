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

package org.apache.hyracks.dataflow.std.buffermanager;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public abstract class AbstractTuplePointerAccessor implements ITuplePointerAccessor {

    protected int tid = -1;

    abstract IFrameTupleAccessor getInnerAccessor();

    abstract void resetInnerAccessor(TuplePointer tuplePointer);

    @Override
    public void reset(TuplePointer tuplePointer) {
        resetInnerAccessor(tuplePointer);
        tid = tuplePointer.getTupleIndex();
    }

    @Override
    public int getTupleStartOffset() {
        return getTupleStartOffset(tid);
    }

    @Override
    public int getTupleLength() {
        return getTupleLength(tid);
    }

    @Override
    public int getAbsFieldStartOffset(int fieldId) {
        return getAbsoluteFieldStartOffset(tid, fieldId);
    }

    @Override
    public int getFieldLength(int fieldId) {
        return getFieldLength(tid, fieldId);
    }

    @Override
    public ByteBuffer getBuffer() {
        return getInnerAccessor().getBuffer();
    }

    @Override
    public int getFieldCount() {
        return getInnerAccessor().getFieldCount();
    }

    @Override
    public int getFieldSlotsLength() {
        return getInnerAccessor().getFieldSlotsLength();
    }

    protected void checkTupleIndex(int tupleIndex) {
        if (tupleIndex != tid) {
            throw new IllegalArgumentException(
                    "ITupleBufferAccessor can not access the different tid other than the one given in reset function");
        }
    }

    @Override
    public int getFieldEndOffset(int tupleIndex, int fIdx) {
        checkTupleIndex(tupleIndex);
        return getInnerAccessor().getFieldEndOffset(tid, fIdx);
    }

    @Override
    public int getFieldStartOffset(int tupleIndex, int fIdx) {
        checkTupleIndex(tupleIndex);
        return getInnerAccessor().getFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public int getFieldLength(int tupleIndex, int fIdx) {
        checkTupleIndex(tupleIndex);
        return getInnerAccessor().getFieldLength(tupleIndex, fIdx);
    }

    @Override
    public int getTupleLength(int tupleIndex) {
        checkTupleIndex(tupleIndex);
        return getInnerAccessor().getTupleLength(tupleIndex);
    }

    @Override
    public int getTupleEndOffset(int tupleIndex) {
        checkTupleIndex(tupleIndex);
        return getInnerAccessor().getTupleEndOffset(tupleIndex);
    }

    @Override
    public int getTupleStartOffset(int tupleIndex) {
        checkTupleIndex(tupleIndex);
        return getInnerAccessor().getTupleStartOffset(tupleIndex);
    }

    @Override
    public int getAbsoluteFieldStartOffset(int tupleIndex, int fIdx) {
        checkTupleIndex(tupleIndex);
        return getInnerAccessor().getAbsoluteFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public int getTupleCount() {
        return getInnerAccessor().getTupleCount();
    }

    @Override
    public void reset(ByteBuffer buffer) {
        throw new IllegalAccessError("Should never call this reset");
    }
}
