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

package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * This is a frame tuple accessor class for inverted list.
 * The frame structure: [4 bytes for minimum Hyracks frame count] [tuple 1] ... [tuple n] ...
 * [4 bytes for the tuple count in a frame]
 *
 * The tuples can be fixed-size or variable-size.
 * This class is mainly used to merge two inverted lists, e.g. searching the conjunction of two keywords "abc" AND "xyz"
 */
public abstract class AbstractInvertedListSearchResultFrameTupleAccessor implements IFrameTupleAccessor {

    protected final int frameSize;
    protected ByteBuffer buffer;

    protected final ITypeTraits[] fields;

    protected abstract void verifyTypeTraits() throws HyracksDataException;

    public AbstractInvertedListSearchResultFrameTupleAccessor(int frameSize, ITypeTraits[] fields)
            throws HyracksDataException {
        this.frameSize = frameSize;
        this.fields = fields;

        verifyTypeTraits();
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public int getFieldCount() {
        return fields.length;
    }

    @Override
    public int getTupleLength(int tupleIndex) {
        return getTupleEndOffset(tupleIndex) - getTupleStartOffset(tupleIndex);
    }

    @Override
    public int getFieldSlotsLength() {
        return 0;
    }

    @Override
    public int getTupleCount() {
        return buffer != null ? buffer.getInt(FrameHelper.getTupleCountOffset(frameSize)) : 0;
    }

    @Override
    public int getTupleEndOffset(int tupleIndex) {
        return getFieldEndOffset(tupleIndex, fields.length - 1);
    }

    @Override
    public int getAbsoluteFieldStartOffset(int tupleIndex, int fIdx) {
        return getTupleStartOffset(tupleIndex) + getFieldSlotsLength() + getFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public void reset(ByteBuffer buffer) {
        this.buffer = buffer;
    }
}
