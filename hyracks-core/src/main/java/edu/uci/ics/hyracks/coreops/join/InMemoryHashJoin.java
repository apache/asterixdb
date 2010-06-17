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
package edu.uci.ics.hyracks.coreops.join;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.context.HyracksContext;

public class InMemoryHashJoin {
    private final Link[] table;
    private final List<ByteBuffer> buffers;
    private final FrameTupleAccessor accessor0;
    private final ITuplePartitionComputer tpc0;
    private final FrameTupleAccessor accessor1;
    private final ITuplePartitionComputer tpc1;
    private final FrameTupleAppender appender;
    private final FrameTuplePairComparator tpComparator;
    private final ByteBuffer outBuffer;

    public InMemoryHashJoin(HyracksContext ctx, int tableSize, FrameTupleAccessor accessor0,
            ITuplePartitionComputer tpc0, FrameTupleAccessor accessor1, ITuplePartitionComputer tpc1,
            FrameTuplePairComparator comparator) {
        table = new Link[tableSize];
        buffers = new ArrayList<ByteBuffer>();
        this.accessor0 = accessor0;
        this.tpc0 = tpc0;
        this.accessor1 = accessor1;
        this.tpc1 = tpc1;
        appender = new FrameTupleAppender(ctx);
        tpComparator = comparator;
        outBuffer = ctx.getResourceManager().allocateFrame();
        appender.reset(outBuffer, true);
    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        buffers.add(buffer);
        int bIndex = buffers.size() - 1;
        accessor0.reset(buffer);
        int tCount = accessor0.getTupleCount();
        for (int i = 0; i < tCount; ++i) {
            int entry = tpc0.partition(accessor0, i, table.length);
            long tPointer = (((long) bIndex) << 32) + i;
            Link link = table[entry];
            if (link == null) {
                link = table[entry] = new Link();
            }
            link.add(tPointer);
        }
    }

    public void join(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessor1.reset(buffer);
        int tupleCount1 = accessor1.getTupleCount();
        for (int i = 0; i < tupleCount1; ++i) {
            int entry = tpc1.partition(accessor1, i, table.length);
            Link link = table[entry];
            if (link != null) {
                for (int j = 0; j < link.size; ++j) {
                    long pointer = link.pointers[j];
                    int bIndex = (int) ((pointer >> 32) & 0xffffffff);
                    int tIndex = (int) (pointer & 0xffffffff);
                    accessor0.reset(buffers.get(bIndex));
                    int c = tpComparator.compare(accessor0, tIndex, accessor1, i);
                    if (c == 0) {
                        if (!appender.appendConcat(accessor0, tIndex, accessor1, i)) {
                            flushFrame(outBuffer, writer);
                            appender.reset(outBuffer, true);
                            if (!appender.appendConcat(accessor0, tIndex, accessor1, i)) {
                                throw new IllegalStateException();
                            }
                        }
                    }
                }
            }
        }
    }

    public void closeJoin(IFrameWriter writer) throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            flushFrame(outBuffer, writer);
        }
    }

    private void flushFrame(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        buffer.position(0);
        buffer.limit(buffer.capacity());
        writer.nextFrame(outBuffer);
        buffer.position(0);
        buffer.limit(buffer.capacity());
    }

    private static class Link {
        private static final int INIT_POINTERS_SIZE = 8;

        long[] pointers;
        int size;

        Link() {
            pointers = new long[INIT_POINTERS_SIZE];
            size = 0;
        }

        void add(long pointer) {
            if (size >= pointers.length) {
                pointers = Arrays.copyOf(pointers, pointers.length * 2);
            }
            pointers[size++] = pointer;
        }
    }
}