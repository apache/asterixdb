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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IConnectionDemultiplexer;
import edu.uci.ics.hyracks.api.comm.IConnectionEntry;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.comm.io.FrameHelper;
import edu.uci.ics.hyracks.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.comm.io.FrameTuplePairComparator;

public class SortMergeFrameReader implements IFrameReader {
    private static final Logger LOGGER = Logger.getLogger(SortMergeFrameReader.class.getName());

    private final IHyracksContext ctx;
    private final IConnectionDemultiplexer demux;
    private final FrameTuplePairComparator tpc;
    private final FrameTupleAppender appender;
    private final RecordDescriptor recordDescriptor;
    private Run[] runs;
    private ByteBuffer[] frames;
    private PriorityQueue<Integer> pQueue;
    private int lastReadSender;
    private boolean first;

    public SortMergeFrameReader(IHyracksContext ctx, IConnectionDemultiplexer demux, int[] sortFields,
            IBinaryComparator[] comparators, RecordDescriptor recordDescriptor) {
        this.ctx = ctx;
        this.demux = demux;
        tpc = new FrameTuplePairComparator(sortFields, sortFields, comparators);
        appender = new FrameTupleAppender(ctx);
        this.recordDescriptor = recordDescriptor;
    }

    @Override
    public void open() throws HyracksDataException {
        int nSenders = demux.getSenderCount();
        runs = new Run[nSenders];
        frames = new ByteBuffer[nSenders];
        for (int i = 0; i < runs.length; ++i) {
            runs[i] = new Run(i);
            frames[i] = ctx.getResourceManager().allocateFrame();
        }
        pQueue = new PriorityQueue<Integer>(nSenders, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                int i1 = o1.intValue();
                int i2 = o2.intValue();
                Run r1 = runs[i1];
                Run r2 = runs[i2];

                int c = tpc.compare(r1.accessor, r1.tIndex, r2.accessor, r2.tIndex);
                return c == 0 ? (i1 < i2 ? -1 : 1) : c;
            }
        });
        lastReadSender = 0;
        first = true;
    }

    @Override
    public void close() throws HyracksDataException {
        for (Run r : runs) {
            r.close();
        }
    }

    @Override
    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
        buffer.clear();
        buffer.position(buffer.capacity());
        appender.reset(buffer, true);
        if (first) {
            for (int i = 0; i < runs.length; ++i) {
                if (runs[i].next()) {
                    pQueue.add(Integer.valueOf(i));
                }
            }
        }
        first = false;
        while (true) {
            if (pQueue.isEmpty()) {
                return appender.getTupleCount() > 0;
            }
            Integer top = pQueue.peek();
            Run run = runs[top.intValue()];
            if (!appender.append(run.accessor, run.tIndex)) {
                return true;
            }
            pQueue.remove();
            if (run.next()) {
                pQueue.add(top);
            }
        }
    }

    private class Run {
        private final int runId;
        private final File file;
        private final FileChannel channel;
        private final ByteBuffer frame;
        private final FrameTupleAccessor accessor;
        private int tIndex;
        private long readFP;
        private long writeFP;
        private boolean eof;

        public Run(int runId) throws HyracksDataException {
            this.runId = runId;
            try {
                file = ctx.getResourceManager().createFile(SortMergeFrameReader.class.getSimpleName(), ".run");
                RandomAccessFile raf = new RandomAccessFile(file, "rw");
                channel = raf.getChannel();
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
            frame = ctx.getResourceManager().allocateFrame();
            accessor = new FrameTupleAccessor(ctx, recordDescriptor);
            readFP = 0;
            writeFP = 0;
            eof = false;
        }

        public void close() throws HyracksDataException {
            try {
                channel.close();
                file.delete();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        private void write(ByteBuffer frame) throws HyracksDataException {
            try {
                int len = frame.capacity();
                while (len > 0) {
                    int sz = channel.write(frame, writeFP);
                    if (sz < 0) {
                        throw new HyracksDataException("Error writing to run");
                    }
                    len -= sz;
                    writeFP += sz;
                }
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        private boolean next() throws HyracksDataException {
            ++tIndex;
            while (readFP == 0 || tIndex >= accessor.getTupleCount()) {
                if (!read(frame)) {
                    return false;
                }
                accessor.reset(frame);
                tIndex = 0;
            }
            return true;
        }

        private boolean read(ByteBuffer frame) throws HyracksDataException {
            while (!eof && readFP >= writeFP) {
                spoolRuns(runId);
            }
            if (eof && readFP >= writeFP) {
                return false;
            }
            try {
                channel.position(readFP);
                frame.clear();
                int len = frame.capacity();
                while (len > 0) {
                    int sz = channel.read(frame, readFP);
                    if (sz < 0) {
                        throw new HyracksDataException("Error reading file");
                    }
                    len -= sz;
                    readFP += sz;
                }
                return true;
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        private void eof() {
            eof = true;
        }
    }

    private void spoolRuns(int interestingRun) throws HyracksDataException {
        while (true) {
            IConnectionEntry entry = demux.findNextReadyEntry(lastReadSender);
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
                runs[lastReadSender].eof();
                netBuffer.clear();
                demux.unreadyEntry(lastReadSender);
                if (openEntries == 0) {
                    return;
                }
            } else {
                runs[lastReadSender].write(netBuffer);
                netBuffer.clear();
                demux.unreadyEntry(lastReadSender);
            }
            if (lastReadSender == interestingRun) {
                return;
            }
        }
    }
}