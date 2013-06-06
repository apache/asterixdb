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
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public class ResultFrameTupleAccessor implements IFrameTupleAccessor {

    private final int frameSize;
    private ByteBuffer buffer;

    public ResultFrameTupleAccessor(int frameSize) {
        this.frameSize = frameSize;
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
        return buffer.getInt(FrameHelper.getTupleCountOffset(frameSize));
    }

    @Override
    public int getTupleStartOffset(int tupleIndex) {
        return tupleIndex == 0 ? 0 : buffer.getInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * tupleIndex);
    }

    @Override
    public int getTupleEndOffset(int tupleIndex) {
        return buffer.getInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * (tupleIndex + 1));
    }

    @Override
    public int getFieldStartOffset(int tupleIndex, int fIdx) {
        return fIdx == 0 ? 0 : buffer.getInt(getTupleStartOffset(tupleIndex) + (fIdx - 1) * 4);
    }

    @Override
    public int getFieldEndOffset(int tupleIndex, int fIdx) {
        return buffer.getInt(getTupleStartOffset(tupleIndex) + fIdx * 4);
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

            bbis.setByteBuffer(buffer, getTupleStartOffset(i));
            System.err.print(dis);

            System.err.println("]");
        }
    }

    @Override
    public int getFieldCount() {
        return 1;
    }
}
