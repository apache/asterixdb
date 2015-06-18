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
