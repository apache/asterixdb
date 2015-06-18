/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.hyracks.dataflow.common.comm.io;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.FrameConstants;
import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.comm.IFrameAppender;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.util.IntSerDeUtils;

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
        return tupleDataEndOffset + FrameHelper.calcSpaceInFrame(fieldCount, tupleLength)
                + tupleCount * FrameConstants.SIZE_LEN
                <= FrameHelper.getTupleCountOffset(frame.getFrameSize());
    }

    private void reset(ByteBuffer buffer, boolean clear) {
        array = buffer.array();
        if (clear) {
            IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()), 0);
            tupleCount = 0;
            tupleDataEndOffset = FrameConstants.TUPLE_START_OFFSET;
        } else {
            tupleCount = IntSerDeUtils.getInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()));
            tupleDataEndOffset = tupleCount == 0 ?
                    FrameConstants.TUPLE_START_OFFSET :
                    IntSerDeUtils.getInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize())
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
    public void flush(IFrameWriter outWriter, boolean clearFrame) throws HyracksDataException {
        getBuffer().clear();
        if (getTupleCount() > 0) {
            outWriter.nextFrame(getBuffer());
        }
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

}
