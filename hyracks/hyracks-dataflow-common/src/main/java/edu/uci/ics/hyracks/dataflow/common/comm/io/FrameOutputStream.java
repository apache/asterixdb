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
package edu.uci.ics.hyracks.dataflow.common.comm.io;

import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

public class FrameOutputStream extends ByteArrayAccessibleOutputStream {
    private static final Logger LOGGER = Logger.getLogger(FrameOutputStream.class.getName());

    private final FrameTupleAppender frameTupleAppender;

    public FrameOutputStream(int frameSize) {
        super(frameSize);
        this.frameTupleAppender = new FrameTupleAppender(frameSize);
    }

    public void reset(ByteBuffer buffer, boolean clear) {
        if (clear) {
            buffer.clear();
        }
        frameTupleAppender.reset(buffer, clear);
    }

    public int getTupleCount() {
        int tupleCount = frameTupleAppender.getTupleCount();
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("appendTuple(): tuple count: " + tupleCount);
        }
        return tupleCount;
    }

    public boolean appendTuple() {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("appendTuple(): tuple size: " + count);
        }
        boolean appended = frameTupleAppender.append(buf, 0, count);
        count = 0;
        return appended;
    }
}
