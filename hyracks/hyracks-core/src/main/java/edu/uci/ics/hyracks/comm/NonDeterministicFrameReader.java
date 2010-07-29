/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.comm;

import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IConnectionDemultiplexer;
import edu.uci.ics.hyracks.api.comm.IConnectionEntry;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.comm.io.FrameHelper;

public class NonDeterministicFrameReader implements IFrameReader {
    private static final Logger LOGGER = Logger.getLogger(NonDeterministicFrameReader.class.getName());

    private final IHyracksContext ctx;
    private final IConnectionDemultiplexer demux;
    private int lastReadSender;
    private boolean eos;

    public NonDeterministicFrameReader(IHyracksContext ctx, IConnectionDemultiplexer demux) {
        this.ctx = ctx;
        this.demux = demux;
    }

    @Override
    public void open() throws HyracksDataException {
        lastReadSender = 0;
        eos = false;
    }

    @Override
    public void close() throws HyracksDataException {
    }

    @Override
    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
        if (eos) {
            return false;
        }
        while (true) {
            IConnectionEntry entry = demux.findNextReadyEntry(lastReadSender);
            if (entry.aborted()) {
                eos = true;
                return false;
            }
            lastReadSender = (Integer) entry.getAttachment();
            ByteBuffer netBuffer = entry.getReadBuffer();
            int tupleCount = netBuffer.getInt(FrameHelper.getTupleCountOffset(ctx));
            if (LOGGER.isLoggable(Level.FINER)) {
                LOGGER.finer("Frame Tuple Count: " + tupleCount);
            }
            if (tupleCount == 0) {
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine("Empty Frame received: Closing " + lastReadSender);
                }
                int openEntries = demux.closeEntry(lastReadSender);
                if (openEntries == 0) {
                    eos = true;
                    return false;
                }
                netBuffer.clear();
                demux.unreadyEntry(lastReadSender);
            } else {
                buffer.clear();
                buffer.put(netBuffer);
                netBuffer.clear();
                demux.unreadyEntry(lastReadSender);
                return true;
            }
        }
    }
}