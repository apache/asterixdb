package edu.uci.ics.asterix.runtime.operators.file;

import java.util.Map;

import edu.uci.ics.asterix.common.parse.ITupleParserPolicy;
import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class RateContolledParserPolicy implements ITupleParserPolicy {

    protected FrameTupleAppender appender;
    protected IFrame  frame;
    private IFrameWriter writer;
    private long interTupleInterval;
    private boolean delayConfigured;

    public static final String INTER_TUPLE_INTERVAL = "tuple-interval";

    public RateContolledParserPolicy() {

    }

    public TupleParserPolicy getType() {
        return ITupleParserPolicy.TupleParserPolicy.FRAME_FULL;
    }

 
    @Override
    public void addTuple(ArrayTupleBuilder tb) throws HyracksDataException {
        if (delayConfigured) {
            try {
                Thread.sleep(interTupleInterval);
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }
        boolean success = appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        if (!success) {
            FrameUtils.flushFrame(frame.getBuffer(), writer);
            appender.reset(frame, true);
            success = appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
            if (!success) {
                throw new IllegalStateException();
            }
        }
        appender.reset(frame, true);
    }

    @Override
    public void close() throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            FrameUtils.flushFrame(frame.getBuffer(), writer);
        }
    }

    @Override
    public void configure(Map<String, String> configuration) throws HyracksDataException {
        String propValue = configuration.get(INTER_TUPLE_INTERVAL);
        if (propValue != null) {
            interTupleInterval = Long.parseLong(propValue);
        } else {
            interTupleInterval = 0;
        }
        delayConfigured = interTupleInterval != 0;
        
    }

    @Override
    public void initialize(IHyracksTaskContext ctx, IFrameWriter writer) throws HyracksDataException {
        this.appender = new FrameTupleAppender();
        this.frame = new VSizeFrame(ctx);
    }
}
