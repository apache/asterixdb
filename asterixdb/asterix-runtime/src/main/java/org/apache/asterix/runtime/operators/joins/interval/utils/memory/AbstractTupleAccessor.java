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

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public abstract class AbstractTupleAccessor implements ITupleAccessor {
    public static final int UNSET = -2;
    public static final int INITIALIZED = -1;

    protected int tupleId = UNSET;

    protected int frameId;

    protected abstract IFrameTupleAccessor getInnerAccessor();

    protected abstract void resetInnerAccessor(int frameId);

    protected abstract void resetInnerAccessor(TuplePointer tp);

    protected abstract int getFrameCount();

    @Override
    public int getTupleStartOffset() {
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

    @Override
    public int getFieldEndOffset(int tupleIndex, int fIdx) {
        return getInnerAccessor().getFieldEndOffset(tupleId, fIdx);
    }

    @Override
    public int getFieldStartOffset(int tupleIndex, int fIdx) {
        return getInnerAccessor().getFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public int getFieldLength(int tupleIndex, int fIdx) {
        return getInnerAccessor().getFieldLength(tupleIndex, fIdx);
    }

    @Override
    public int getTupleLength(int tupleIndex) {
        return getInnerAccessor().getTupleLength(tupleIndex);
    }

    @Override
    public int getTupleEndOffset(int tupleIndex) {
        return getInnerAccessor().getTupleEndOffset(tupleIndex);
    }

    @Override
    public int getTupleStartOffset(int tupleIndex) {
        return getInnerAccessor().getTupleStartOffset(tupleIndex);
    }

    @Override
    public int getAbsoluteFieldStartOffset(int tupleIndex, int fIdx) {
        return getInnerAccessor().getAbsoluteFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public int getTupleCount() {
        return getInnerAccessor().getTupleCount();
    }

    @Override
    public void reset(TuplePointer tuplePointer) {
        resetInnerAccessor(tuplePointer.getFrameIndex());
    }

    @Override
    public void reset(ByteBuffer buffer) {
        throw new IllegalAccessError("Should never call this reset");
    }

    @Override
    public int getTupleEndOffset() {
        return getInnerAccessor().getTupleEndOffset(tupleId);
    }

    @Override
    public int getFieldEndOffset(int fieldId) {
        return getInnerAccessor().getFieldEndOffset(tupleId, fieldId);
    }

    @Override
    public int getFieldStartOffset(int fieldId) {
        return getInnerAccessor().getFieldStartOffset(tupleId, fieldId);
    }

    @Override
    public void getTuplePointer(TuplePointer tp) {
        tp.reset(frameId, tupleId);
    }

    @Override
    public int getTupleId() {
        return tupleId;
    }

    @Override
    public void setTupleId(int tupleId) {
        this.tupleId = tupleId;
    }

    @Override
    public void reset() {
        tupleId = INITIALIZED;
        frameId = 0;
        resetInnerAccessor(frameId);
    }

    @Override
    public boolean hasNext() {
        if (tupleId + 1 < getTupleCount() || frameId + 1 < getFrameCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean exists() {
        return INITIALIZED < tupleId && getTupleEndOffset(tupleId) > 0 && tupleId < getTupleCount()
                && frameId < getFrameCount();
    }

    @Override
    public void next() {
        // TODO Consider error messages
        if (tupleId + 1 < getTupleCount()) {
            ++tupleId;
        } else if (frameId + 1 < getFrameCount()) {
            ++frameId;
            resetInnerAccessor(frameId);
            tupleId = 0;
        } else {
            // Force exists to fail, by incrementing the tuple pointer.
            ++tupleId;
        }
    }

}
