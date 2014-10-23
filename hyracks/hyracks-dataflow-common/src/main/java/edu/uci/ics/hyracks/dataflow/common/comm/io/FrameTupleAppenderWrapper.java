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

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

/**
 * This class wraps the calls of FrameTupleAppender and
 * allows user to not worry about flushing full frames.
 * TODO(yingyib): cleanup existing usage of FrameTupleAppender.
 * 
 * @author yingyib
 */
public class FrameTupleAppenderWrapper {
    private final FrameTupleAppender frameTupleAppender;
    private final ByteBuffer outputFrame;
    private final IFrameWriter outputWriter;

    public FrameTupleAppenderWrapper(FrameTupleAppender frameTupleAppender, ByteBuffer outputFrame,
            IFrameWriter outputWriter) {
        this.frameTupleAppender = frameTupleAppender;
        this.outputFrame = outputFrame;
        this.outputWriter = outputWriter;
    }

    public void open() throws HyracksDataException {
        outputWriter.open();
    }

    public void flush() throws HyracksDataException {
        if (frameTupleAppender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outputFrame, outputWriter);
        }
    }

    public void close() throws HyracksDataException {
        outputWriter.close();
    }

    public void fail() throws HyracksDataException {
        outputWriter.fail();
    }

    public void reset(ByteBuffer buffer, boolean clear) {
        frameTupleAppender.reset(buffer, clear);
    }

    public void appendSkipEmptyField(int[] fieldSlots, byte[] bytes, int offset, int length)
            throws HyracksDataException {
        if (!frameTupleAppender.append(fieldSlots, bytes, offset, length)) {
            FrameUtils.flushFrame(outputFrame, outputWriter);
            frameTupleAppender.reset(outputFrame, true);
            if (!frameTupleAppender.appendSkipEmptyField(fieldSlots, bytes, offset, length)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
    }

    public void append(byte[] bytes, int offset, int length) throws HyracksDataException {
        if (!frameTupleAppender.append(bytes, offset, length)) {
            FrameUtils.flushFrame(outputFrame, outputWriter);
            frameTupleAppender.reset(outputFrame, true);
            if (!frameTupleAppender.append(bytes, offset, length)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
    }

    public void append(IFrameTupleAccessor tupleAccessor, int tStartOffset, int tEndOffset) throws HyracksDataException {
        if (!frameTupleAppender.append(tupleAccessor, tStartOffset, tEndOffset)) {
            FrameUtils.flushFrame(outputFrame, outputWriter);
            frameTupleAppender.reset(outputFrame, true);
            if (!frameTupleAppender.append(tupleAccessor, tStartOffset, tEndOffset)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
    }

    public void append(IFrameTupleAccessor tupleAccessor, int tIndex) throws HyracksDataException {
        if (!frameTupleAppender.append(tupleAccessor, tIndex)) {
            FrameUtils.flushFrame(outputFrame, outputWriter);
            frameTupleAppender.reset(outputFrame, true);
            if (!frameTupleAppender.append(tupleAccessor, tIndex)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
    }

    public void appendConcat(IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1)
            throws HyracksDataException {
        if (!frameTupleAppender.appendConcat(accessor0, tIndex0, accessor1, tIndex1)) {
            FrameUtils.flushFrame(outputFrame, outputWriter);
            frameTupleAppender.reset(outputFrame, true);
            if (!frameTupleAppender.appendConcat(accessor0, tIndex0, accessor1, tIndex1)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
    }

    public void appendConcat(IFrameTupleAccessor accessor0, int tIndex0, int[] fieldSlots1, byte[] bytes1, int offset1,
            int dataLen1) throws HyracksDataException {
        if (!frameTupleAppender.appendConcat(accessor0, tIndex0, fieldSlots1, bytes1, offset1, dataLen1)) {
            FrameUtils.flushFrame(outputFrame, outputWriter);
            frameTupleAppender.reset(outputFrame, true);
            if (!frameTupleAppender.appendConcat(accessor0, tIndex0, fieldSlots1, bytes1, offset1, dataLen1)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
    }

    public void appendProjection(IFrameTupleAccessor accessor, int tIndex, int[] fields) throws HyracksDataException {
        if (!frameTupleAppender.appendProjection(accessor, tIndex, fields)) {
            FrameUtils.flushFrame(outputFrame, outputWriter);
            frameTupleAppender.reset(outputFrame, true);
            if (!frameTupleAppender.appendProjection(accessor, tIndex, fields)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
    }

}
