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

import edu.uci.ics.hyracks.api.comm.IFrameFieldAppender;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAppender;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameFixedFieldTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public abstract class AbstractOneInputOneOutputOneFieldFramePushRuntime
        extends AbstractOneInputOneOutputOneFramePushRuntime {

    @Override
    protected IFrameTupleAppender getTupleAppender() {
        return (FrameFixedFieldTupleAppender) appender;
    }

    protected IFrameFieldAppender getFieldAppender() {
        return (FrameFixedFieldTupleAppender) appender;
    }

    protected final void initAccessAppendFieldRef(IHyracksTaskContext ctx) throws HyracksDataException {
        frame = new VSizeFrame(ctx);
        appender = new FrameFixedFieldTupleAppender(inputRecordDesc.getFieldCount());
        appender.reset(frame, true);
        tAccess = new FrameTupleAccessor(inputRecordDesc);
        tRef = new FrameTupleReference();
    }

    protected void appendField(byte[] array, int start, int length) throws HyracksDataException {
        FrameUtils.appendFieldToWriter(writer, getFieldAppender(), array, start, length);
    }

    protected void appendField(IFrameTupleAccessor accessor, int tid, int fid) throws HyracksDataException {
        FrameUtils.appendFieldToWriter(writer, getFieldAppender(), accessor, tid, fid);
    }
}
