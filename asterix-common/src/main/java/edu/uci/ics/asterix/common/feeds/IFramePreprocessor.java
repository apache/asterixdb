package edu.uci.ics.asterix.common.feeds;

import java.nio.ByteBuffer;

public interface IFramePreprocessor {

    public void preProcess(ByteBuffer frame) throws Exception;
}
