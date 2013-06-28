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
import edu.uci.ics.hyracks.api.channels.IInputChannelMonitor;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class InputChannelFrameReader implements IFrameReader, IInputChannelMonitor {
    private final IInputChannel channel;

    private int availableFrames;

    private boolean eos;

    private boolean failed;

    public InputChannelFrameReader(IInputChannel channel) {
        this.channel = channel;
        availableFrames = 0;
        eos = false;
        failed = false;
    }

    @Override
    public void open() throws HyracksDataException {
    }

    @Override
    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
        synchronized (this) {
            while (!failed && !eos && availableFrames <= 0) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new HyracksDataException(e);
                }
            }
            if (failed) {
                throw new HyracksDataException("Failure occurred on input");
            }
            if (availableFrames <= 0 && eos) {
                return false;
            }
            --availableFrames;
        }
        ByteBuffer srcBuffer = channel.getNextBuffer();
        FrameUtils.copy(srcBuffer, buffer);
        channel.recycleBuffer(srcBuffer);
        return true;
    }

    @Override
    public void close() throws HyracksDataException {

    }

    @Override
    public synchronized void notifyFailure(IInputChannel channel) {
        failed = true;
        notifyAll();
    }

    @Override
    public synchronized void notifyDataAvailability(IInputChannel channel, int nFrames) {
        availableFrames += nFrames;
        notifyAll();
    }

    @Override
    public synchronized void notifyEndOfStream(IInputChannel channel) {
        eos = true;
        notifyAll();
    }
}