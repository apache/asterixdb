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
package edu.uci.ics.hyracks.dataflow.std.collectors;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class NonDeterministicFrameReader implements IFrameReader {
    private final NonDeterministicChannelReader channelReader;

    public NonDeterministicFrameReader(NonDeterministicChannelReader channelReader) {
        this.channelReader = channelReader;
    }

    @Override
    public void open() throws HyracksDataException {
        channelReader.open();
    }

    @Override
    public boolean nextFrame(IFrame frame) throws HyracksDataException {
        int index = channelReader.findNextSender();
        if (index < 0) {
            return false;
        }
        frame.reset();
        ByteBuffer srcFrame = channelReader.getNextBuffer(index);
        int nBlocks = FrameHelper.deserializeNumOfMinFrame(srcFrame);
        frame.ensureFrameSize(frame.getMinSize() * nBlocks);
        FrameUtils.copyWholeFrame(srcFrame, frame.getBuffer());
        channelReader.recycleBuffer(index, srcFrame);
        for (int i = 1; i < nBlocks; ++i) {
            srcFrame = channelReader.getNextBuffer(index);
            frame.getBuffer().put(srcFrame);
            channelReader.recycleBuffer(index, srcFrame);
        }
        if (frame.getBuffer().hasRemaining()) { // bigger frame
            FrameHelper.clearRemainingFrame(frame.getBuffer(), frame.getBuffer().position());
        }
        frame.getBuffer().flip();
        return true;
    }

    @Override
    public synchronized void close() throws HyracksDataException {
        channelReader.close();
    }
}