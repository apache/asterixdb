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
package org.apache.hyracks.algebricks.runtime.operators.base;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameAppender;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public abstract class AbstractOneInputOneOutputOneFramePushRuntime extends AbstractOneInputOneOutputPushRuntime {

    protected IFrameAppender appender;
    protected IFrame frame;
    protected FrameTupleAccessor tAccess;
    protected FrameTupleReference tRef;

    protected final void initAccessAppend(IHyracksTaskContext ctx) throws HyracksDataException {
        frame = new VSizeFrame(ctx);
        appender = new FrameTupleAppender(frame);
        tAccess = new FrameTupleAccessor(inputRecordDesc);
    }

    protected final void initAccessAppendRef(IHyracksTaskContext ctx) throws HyracksDataException {
        initAccessAppend(ctx);
        tRef = new FrameTupleReference();
    }

    @Override
    public void close() throws HyracksDataException {
        if (!isOpen) {
            return;
        }
        Throwable closeException = null;
        try {
            flushIfNotFailed();
        } catch (Exception e) {
            closeException = e;
            fail(closeException);
        } finally {
            closeException = CleanupUtils.close(writer, closeException);
        }
        if (closeException != null) {
            throw HyracksDataException.create(closeException);
        }
    }

    protected void flushAndReset() throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            appender.write(writer, true);
        }
    }

    protected void flushIfNotFailed() throws HyracksDataException {
        if (!failed) {
            flushAndReset();
        }
    }

    protected IFrameTupleAppender getTupleAppender() {
        return (FrameTupleAppender) appender;
    }

    protected void appendToFrameFromTupleBuilder(ArrayTupleBuilder tb) throws HyracksDataException {
        appendToFrameFromTupleBuilder(tb, false);
    }

    protected void appendToFrameFromTupleBuilder(ArrayTupleBuilder tb, boolean flushFrame) throws HyracksDataException {
        FrameUtils.appendToWriter(writer, getTupleAppender(), tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                tb.getSize());
        if (flushFrame) {
            flushAndReset();
        }
    }

    protected void appendProjectionToFrame(int tIndex, int[] projectionList) throws HyracksDataException {
        appendProjectionToFrame(tIndex, projectionList, false);
    }

    protected void appendProjectionToFrame(int tIndex, int[] projectionList, boolean flushFrame)
            throws HyracksDataException {
        FrameUtils.appendProjectionToWriter(writer, getTupleAppender(), tAccess, tIndex, projectionList);
        if (flushFrame) {
            flushAndReset();
        }
    }

    protected void appendTupleToFrame(int tIndex) throws HyracksDataException {
        FrameUtils.appendToWriter(writer, getTupleAppender(), tAccess, tIndex);
    }

    protected void appendConcat(IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1)
            throws HyracksDataException {
        FrameUtils.appendConcatToWriter(writer, getTupleAppender(), accessor0, tIndex0, accessor1, tIndex1);
    }
}
