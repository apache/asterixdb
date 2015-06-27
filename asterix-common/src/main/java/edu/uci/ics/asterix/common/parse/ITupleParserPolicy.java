package edu.uci.ics.asterix.common.parse;

import java.util.Map;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public interface ITupleParserPolicy {

    public enum TupleParserPolicy {
        FRAME_FULL,
        TIME_COUNT_ELAPSED,
        RATE_CONTROLLED
    }

    public TupleParserPolicy getType();

    public void configure(Map<String, String> configuration) throws HyracksDataException;

    public void initialize(IHyracksTaskContext ctx, IFrameWriter writer) throws HyracksDataException;

    public void addTuple(ArrayTupleBuilder tb) throws HyracksDataException;

    public void close() throws HyracksDataException;
}
