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
package edu.uci.ics.hyracks.dataflow.common.comm.io;

import java.io.DataInputStream;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.common.util.IntSerDeUtils;

/**
 * FrameTupleCursor is used to navigate over tuples in a Frame. A frame is
 * formatted with tuple data concatenated starting at offset 0, one tuple after
 * another. Offset FS - 4 holds an int indicating the number of tuples (N) in
 * the frame. FS - ((i + 1) * 4) for i from 0 to N - 1 holds an int indicating
 * the offset of the (i + 1)^th tuple. Every tuple is organized as a sequence of
 * ints indicating the end of each field in the tuple relative to the end of the
 * field slots.
 * 
 * @author vinayakb
 */
public final class FrameTupleAccessor implements IFrameTupleAccessor {
    private final int frameSize;
    private final RecordDescriptor recordDescriptor;

    private ByteBuffer buffer;

    public FrameTupleAccessor(int frameSize, RecordDescriptor recordDescriptor) {
        this.frameSize = frameSize;
        this.recordDescriptor = recordDescriptor;
    }

    @Override
    public void reset(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public int getTupleCount() {
        return IntSerDeUtils.getInt(buffer.array(), FrameHelper.getTupleCountOffset(frameSize));
    }

    @Override
    public int getTupleStartOffset(int tupleIndex) {
        return tupleIndex == 0 ? 0 : IntSerDeUtils.getInt(buffer.array(), FrameHelper.getTupleCountOffset(frameSize)
                - 4 * tupleIndex);
    }

    @Override
    public int getTupleEndOffset(int tupleIndex) {
        return IntSerDeUtils.getInt(buffer.array(), FrameHelper.getTupleCountOffset(frameSize) - 4 * (tupleIndex + 1));
    }

    @Override
    public int getFieldStartOffset(int tupleIndex, int fIdx) {
        return fIdx == 0 ? 0 : IntSerDeUtils.getInt(buffer.array(), getTupleStartOffset(tupleIndex) + (fIdx - 1) * 4);
    }

    @Override
    public int getFieldEndOffset(int tupleIndex, int fIdx) {
        return IntSerDeUtils.getInt(buffer.array(), getTupleStartOffset(tupleIndex) + fIdx * 4);
    }

    @Override
    public int getFieldLength(int tupleIndex, int fIdx) {
        return getFieldEndOffset(tupleIndex, fIdx) - getFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public int getFieldSlotsLength() {
        return getFieldCount() * 4;
    }

    public void prettyPrint() {
        ByteBufferInputStream bbis = new ByteBufferInputStream();
        DataInputStream dis = new DataInputStream(bbis);
        int tc = getTupleCount();
        System.err.println("TC: " + tc);
        for (int i = 0; i < tc; ++i) {
            System.err.print(i + ":(" + getTupleStartOffset(i) + ", " + getTupleEndOffset(i) + ")[");
            for (int j = 0; j < getFieldCount(); ++j) {
                System.err.print(j + ":(" + getFieldStartOffset(i, j) + ", " + getFieldEndOffset(i, j) + ") ");
                System.err.print("{");
                bbis.setByteBuffer(buffer, getTupleStartOffset(i) + getFieldSlotsLength() + getFieldStartOffset(i, j));
                try {
                    System.err.print(recordDescriptor.getFields()[j].deserialize(dis));
                } catch (HyracksDataException e) {
                    e.printStackTrace();
                }
                System.err.print("}");
            }
            System.err.println("]");
        }
    }

    @Override
    public int getFieldCount() {
        return recordDescriptor.getFieldCount();
    }
}