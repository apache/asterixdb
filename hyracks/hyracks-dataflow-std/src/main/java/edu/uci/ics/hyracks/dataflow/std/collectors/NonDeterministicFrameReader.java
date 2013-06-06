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

import edu.uci.ics.hyracks.api.channels.IInputChannel;
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
    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
        int index = channelReader.findNextSender();
        if (index >= 0) {
            IInputChannel[] channels = channelReader.getChannels();
            ByteBuffer srcFrame = channels[index].getNextBuffer();
            FrameUtils.copy(srcFrame, buffer);
            channels[index].recycleBuffer(srcFrame);
            return true;
        }
        return false;
    }

    @Override
    public synchronized void close() throws HyracksDataException {
        channelReader.close();
    }
}