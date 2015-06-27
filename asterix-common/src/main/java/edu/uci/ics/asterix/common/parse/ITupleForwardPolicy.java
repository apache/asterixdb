package edu.uci.ics.asterix.common.parse;

import java.util.Map;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public interface ITupleForwardPolicy {

    public static final String PARSER_POLICY = "parser-policy";
    
    public enum TupleForwardPolicyType {
        FRAME_FULL,
        COUNTER_TIMER_EXPIRED,
        RATE_CONTROLLED
    }

    public void configure(Map<String, String> configuration);

    public void initialize(IHyracksTaskContext ctx, IFrameWriter frameWriter) throws HyracksDataException;

    public TupleForwardPolicyType getType();

    public void addTuple(ArrayTupleBuilder tb) throws HyracksDataException;

    public void close() throws HyracksDataException;

}
