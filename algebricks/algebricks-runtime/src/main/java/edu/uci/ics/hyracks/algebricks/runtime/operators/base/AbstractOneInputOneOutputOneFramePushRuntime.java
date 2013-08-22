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
package edu.uci.ics.hyracks.algebricks.runtime.operators.base;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public abstract class AbstractOneInputOneOutputOneFramePushRuntime extends AbstractOneInputOneOutputPushRuntime {

    protected FrameTupleAppender appender;
    protected ByteBuffer frame;
    protected FrameTupleAccessor tAccess;
    protected FrameTupleReference tRef;

    @Override
    public void close() throws HyracksDataException {
        if (!failed) {
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(frame, writer);
            }
        }
        writer.close();
        appender.reset(frame, true);
    }

    protected void appendToFrameFromTupleBuilder(ArrayTupleBuilder tb) throws HyracksDataException {
        appendToFrameFromTupleBuilder(tb, false);
    }

    protected void appendToFrameFromTupleBuilder(ArrayTupleBuilder tb, boolean flushFrame) throws HyracksDataException {
        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            FrameUtils.flushFrame(frame, writer);
            appender.reset(frame, true);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new HyracksDataException(
                        "Could not write frame: the size of the tuple is too long to be fit into a single frame. (AbstractOneInputOneOutputOneFramePushRuntime.appendToFrameFromTupleBuilder)");
            }
        }
        if (flushFrame) {
            FrameUtils.flushFrame(frame, writer);
            appender.reset(frame, true);
        }
    }

    protected void appendProjectionToFrame(int tIndex, int[] projectionList) throws HyracksDataException {
        appendProjectionToFrame(tIndex, projectionList, false);
    }

    protected void appendProjectionToFrame(int tIndex, int[] projectionList, boolean flushFrame)
            throws HyracksDataException {
        if (!appender.appendProjection(tAccess, tIndex, projectionList)) {
            FrameUtils.flushFrame(frame, writer);
            appender.reset(frame, true);
            if (!appender.appendProjection(tAccess, tIndex, projectionList)) {
                throw new IllegalStateException(
                        "Could not write frame (AbstractOneInputOneOutputOneFramePushRuntime.appendProjectionToFrame).");
            }
            return;
        }
        if (flushFrame) {
            FrameUtils.flushFrame(frame, writer);
            appender.reset(frame, true);
        }
    }

    protected void appendTupleToFrame(int tIndex) throws HyracksDataException {
        if (!appender.append(tAccess, tIndex)) {
            FrameUtils.flushFrame(frame, writer);
            appender.reset(frame, true);
            if (!appender.append(tAccess, tIndex)) {
                throw new IllegalStateException(
                        "Could not write frame (AbstractOneInputOneOutputOneFramePushRuntime.appendTupleToFrame).");
            }
        }
    }

    protected final void initAccessAppend(IHyracksTaskContext ctx) throws HyracksDataException {
        // if (allocFrame) {
        frame = ctx.allocateFrame();
        appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(frame, true);
        // }
        tAccess = new FrameTupleAccessor(ctx.getFrameSize(), inputRecordDesc);
    }

    protected final void initAccessAppendRef(IHyracksTaskContext ctx) throws HyracksDataException {
        initAccessAppend(ctx);
        tRef = new FrameTupleReference();
    }

}
