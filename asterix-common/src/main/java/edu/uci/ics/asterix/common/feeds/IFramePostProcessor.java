package edu.uci.ics.asterix.common.feeds;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public interface IFramePostProcessor {

    public void postProcessFrame(ByteBuffer frame, FrameTupleAccessor frameAccessor);
}
