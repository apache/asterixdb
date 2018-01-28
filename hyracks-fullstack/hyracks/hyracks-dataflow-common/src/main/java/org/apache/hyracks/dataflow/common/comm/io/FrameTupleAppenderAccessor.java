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

import java.io.DataInputStream;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.FrameConstants;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.util.IntSerDeUtils;

public class FrameTupleAppenderAccessor extends FrameTupleAppender implements IFrameTupleAccessor {
    private int tupleCountOffset;
    private final RecordDescriptor recordDescriptor;

    public FrameTupleAppenderAccessor(RecordDescriptor recordDescriptor) {
        super();
        this.recordDescriptor = recordDescriptor;
    }

    @Override
    public void reset(ByteBuffer buffer) {
        throw new IllegalAccessError("should not call this function");
    }

    @Override
    public int getTupleStartOffset(int tupleIndex) {
        int offset = tupleIndex == 0 ? FrameConstants.TUPLE_START_OFFSET
                : IntSerDeUtils.getInt(getBuffer().array(), tupleCountOffset - 4 * tupleIndex);
        return offset;
    }

    @Override
    public int getAbsoluteFieldStartOffset(int tupleIndex, int fIdx) {
        return getTupleStartOffset(tupleIndex) + getFieldSlotsLength() + getFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public int getTupleEndOffset(int tupleIndex) {
        return IntSerDeUtils.getInt(getBuffer().array(), tupleCountOffset - 4 * (tupleIndex + 1));
    }

    @Override
    public int getFieldStartOffset(int tupleIndex, int fIdx) {
        return fIdx == 0 ? 0
                : IntSerDeUtils.getInt(getBuffer().array(), getTupleStartOffset(tupleIndex) + (fIdx - 1) * 4);
    }

    @Override
    public int getFieldEndOffset(int tupleIndex, int fIdx) {
        return IntSerDeUtils.getInt(getBuffer().array(), getTupleStartOffset(tupleIndex) + fIdx * 4);
    }

    @Override
    public int getFieldLength(int tupleIndex, int fIdx) {
        return getFieldEndOffset(tupleIndex, fIdx) - getFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public int getTupleLength(int tupleIndex) {
        return getTupleEndOffset(tupleIndex) - getTupleStartOffset(tupleIndex);
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
            prettyPrint(i, bbis, dis);
        }
    }

    protected void prettyPrint(int tid, ByteBufferInputStream bbis, DataInputStream dis) {
        System.err.print("tid" + tid + ":(" + getTupleStartOffset(tid) + ", " + getTupleEndOffset(tid) + ")[");
        for (int j = 0; j < getFieldCount(); ++j) {
            System.err.print("f" + j + ":(" + getFieldStartOffset(tid, j) + ", " + getFieldEndOffset(tid, j) + ") ");
            System.err.print("{");
            bbis.setByteBuffer(getBuffer(),
                    getTupleStartOffset(tid) + getFieldSlotsLength() + getFieldStartOffset(tid, j));
            try {
                System.err.print(recordDescriptor.getFields()[j].deserialize(dis));
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }
            System.err.print("}");
        }
        System.err.println();
    }

    public void prettyPrint(int tid) {
        ByteBufferInputStream bbis = new ByteBufferInputStream();
        DataInputStream dis = new DataInputStream(bbis);
        prettyPrint(tid, bbis, dis);
    }

    @Override
    public int getFieldCount() {
        return recordDescriptor.getFieldCount();
    }

    @Override
    public void reset(IFrame frame, boolean clear) throws HyracksDataException {
        super.reset(frame, clear);
        this.tupleCountOffset = FrameHelper.getTupleCountOffset(frame.getFrameSize());
    }
}
