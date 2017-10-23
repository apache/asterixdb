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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.FrameConstants;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.IntSerDeUtils;
import org.apache.hyracks.util.trace.ITracer;

/*
 * Frame
 *  _____________________________________________
 * |[tuple1][tuple2][tuple3].........            |
 * |                      .                      |
 * |                      .                      |
 * |                      .                      |
 * |                      .                      |
 * |                      .                      |
 * |..[tupleN][tuplesOffsets(4*N)][tupleCount(4)]|
 * |_____________________________________________|
 */
public class AbstractFrameAppender implements IFrameAppender {
    protected IFrame frame;
    protected byte[] array; // cached the getBuffer().array to speed up byte array access a little

    protected int tupleCount;
    protected int tupleDataEndOffset;

    @Override
    public void reset(IFrame frame, boolean clear) throws HyracksDataException {
        this.frame = frame;
        if (clear) {
            this.frame.reset();
        }
        reset(getBuffer(), clear);
    }

    protected boolean hasEnoughSpace(int fieldCount, int tupleLength) {
        return tupleDataEndOffset + FrameHelper.calcRequiredSpace(fieldCount, tupleLength)
                + tupleCount * FrameConstants.SIZE_LEN <= FrameHelper.getTupleCountOffset(frame.getFrameSize());
    }

    protected void reset(ByteBuffer buffer, boolean clear) {
        array = buffer.array();
        if (clear) {
            IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()), 0);
            tupleCount = 0;
            tupleDataEndOffset = FrameConstants.TUPLE_START_OFFSET;
        } else {
            tupleCount = IntSerDeUtils.getInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()));
            tupleDataEndOffset = tupleCount == 0 ? FrameConstants.TUPLE_START_OFFSET
                    : IntSerDeUtils.getInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize())
                            - tupleCount * FrameConstants.SIZE_LEN);
        }
    }

    @Override
    public int getTupleCount() {
        return tupleCount;
    }

    @Override
    public ByteBuffer getBuffer() {
        return frame.getBuffer();
    }

    @Override
    public void write(IFrameWriter outWriter, boolean clearFrame) throws HyracksDataException {
        getBuffer().clear();
        outWriter.nextFrame(getBuffer());
        if (clearFrame) {
            frame.reset();
            reset(getBuffer(), true);
        }
    }

    protected boolean canHoldNewTuple(int fieldCount, int dataLength) throws HyracksDataException {
        if (hasEnoughSpace(fieldCount, dataLength)) {
            return true;
        }
        if (tupleCount == 0) {
            frame.ensureFrameSize(FrameHelper.calcAlignedFrameSizeToStore(fieldCount, dataLength, frame.getMinSize()));
            reset(frame.getBuffer(), true);
            return true;
        }
        return false;
    }

    @Override
    public void flush(IFrameWriter writer) throws HyracksDataException {
        if (tupleCount > 0) {
            write(writer, true);
        }
        writer.flush();
    }

    public void flush(IFrameWriter writer, ITracer tracer, String name, long traceCategory, String args)
            throws HyracksDataException {
        final long tid = tracer.durationB(name, traceCategory, args);
        try {
            flush(writer);
        } finally {
            tracer.durationE(tid, traceCategory, args);
        }
    }
}
