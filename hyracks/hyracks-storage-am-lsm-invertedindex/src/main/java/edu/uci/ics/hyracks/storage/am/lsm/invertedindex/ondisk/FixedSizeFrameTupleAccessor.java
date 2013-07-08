/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;

public class FixedSizeFrameTupleAccessor implements IFrameTupleAccessor {

    private final int frameSize;
    private ByteBuffer buffer;

    private final ITypeTraits[] fields;
    private final int[] fieldStartOffsets;
    private final int tupleSize;

    public FixedSizeFrameTupleAccessor(int frameSize, ITypeTraits[] fields) {
        this.frameSize = frameSize;
        this.fields = fields;
        this.fieldStartOffsets = new int[fields.length];
        this.fieldStartOffsets[0] = 0;
        for (int i = 1; i < fields.length; i++) {
            fieldStartOffsets[i] = fieldStartOffsets[i - 1] + fields[i - 1].getFixedLength();
        }

        int tmp = 0;
        for (int i = 0; i < fields.length; i++) {
            tmp += fields[i].getFixedLength();
        }
        tupleSize = tmp;
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
    public int getFieldEndOffset(int tupleIndex, int fIdx) {
        return getTupleStartOffset(tupleIndex) + fieldStartOffsets[fIdx] + fields[fIdx].getFixedLength();
    }

    @Override
    public int getFieldLength(int tupleIndex, int fIdx) {
        return fields[fIdx].getFixedLength();
    }

    @Override
    public int getFieldSlotsLength() {
        return 0;
    }

    @Override
    public int getFieldStartOffset(int tupleIndex, int fIdx) {
        return tupleIndex * tupleSize + fieldStartOffsets[fIdx];
    }

    @Override
    public int getTupleCount() {
        return buffer.getInt(FrameHelper.getTupleCountOffset(frameSize));
    }

    @Override
    public int getTupleEndOffset(int tupleIndex) {
        return getFieldEndOffset(tupleIndex, fields.length - 1);
    }

    @Override
    public int getTupleStartOffset(int tupleIndex) {
        return tupleIndex * tupleSize;
    }

    @Override
    public void reset(ByteBuffer buffer) {
        this.buffer = buffer;
    }
}
