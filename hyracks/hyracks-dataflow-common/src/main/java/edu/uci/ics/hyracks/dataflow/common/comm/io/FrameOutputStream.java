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

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;

public class FrameOutputStream extends ByteArrayAccessibleOutputStream {
    private static final Logger LOGGER = Logger.getLogger(FrameOutputStream.class.getName());

    private final FrameTupleAppender frameTupleAppender;

    public FrameOutputStream(int initialStreamCapaciy) {
        super(initialStreamCapaciy);
        this.frameTupleAppender = new FrameTupleAppender();
    }

    public void reset(IFrame frame, boolean clear) throws HyracksDataException {
        frameTupleAppender.reset(frame, clear);
    }

    public int getTupleCount() {
        int tupleCount = frameTupleAppender.getTupleCount();
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("appendTuple(): tuple count: " + tupleCount);
        }
        return tupleCount;
    }

    public boolean appendTuple() throws HyracksDataException {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("appendTuple(): tuple size: " + count);
        }
        boolean appended = frameTupleAppender.append(buf, 0, count);
        count = 0;
        return appended;
    }

    public void flush(IFrameWriter writer) throws HyracksDataException {
        frameTupleAppender.flush(writer, true);
    }
}
