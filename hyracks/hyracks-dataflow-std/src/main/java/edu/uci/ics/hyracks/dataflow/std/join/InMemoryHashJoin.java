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
package edu.uci.ics.hyracks.dataflow.std.join;

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;

public class InMemoryHashJoin {
    private final Link[] table;
    private final List<ByteBuffer> buffers;
    private final FrameTupleAccessor accessorBuild;
    private final ITuplePartitionComputer tpcBuild;
    private final FrameTupleAccessor accessorProbe;
    private final ITuplePartitionComputer tpcProbe;
    private final FrameTupleAppender appender;
    private final FrameTuplePairComparator tpComparator;
    private final ByteBuffer outBuffer;
    private final boolean isLeftOuter;
    private final ArrayTupleBuilder nullTupleBuild;
    
    public InMemoryHashJoin(IHyracksStageletContext ctx, int tableSize, FrameTupleAccessor accessor0,
            ITuplePartitionComputer tpc0, FrameTupleAccessor accessor1, ITuplePartitionComputer tpc1,
            FrameTuplePairComparator comparator, boolean isLeftOuter, INullWriter[] nullWriters1)
            throws HyracksDataException {
        table = new Link[tableSize];
        buffers = new ArrayList<ByteBuffer>();
        this.accessorBuild = accessor1;
        this.tpcBuild = tpc1;
        this.accessorProbe = accessor0;
        this.tpcProbe = tpc0;
        appender = new FrameTupleAppender(ctx.getFrameSize());
        tpComparator = comparator;
        outBuffer = ctx.allocateFrame();
        appender.reset(outBuffer, true);
        this.isLeftOuter = isLeftOuter;        
        if (isLeftOuter) {
            int fieldCountOuter = accessor1.getFieldCount();
            nullTupleBuild = new ArrayTupleBuilder(fieldCountOuter);
            DataOutput out = nullTupleBuild.getDataOutput();
            for (int i = 0; i < fieldCountOuter; i++) {
                nullWriters1[i].writeNull(out);
                nullTupleBuild.addFieldEndOffset();
            }
        } else {
            nullTupleBuild = null;
        }
    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        buffers.add(buffer);
        int bIndex = buffers.size() - 1;
        accessorBuild.reset(buffer);
        int tCount = accessorBuild.getTupleCount();
        for (int i = 0; i < tCount; ++i) {
            int entry = tpcBuild.partition(accessorBuild, i, table.length);
            long tPointer = (((long) bIndex) << 32) + i;
            Link link = table[entry];
            if (link == null) {
                link = table[entry] = new Link();
            }
            link.add(tPointer);
        }
    }

    public void join(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount0 = accessorProbe.getTupleCount();
        for (int i = 0; i < tupleCount0; ++i) {
            int entry = tpcProbe.partition(accessorProbe, i, table.length);
            Link link = table[entry];
            boolean matchFound = false;
            if (link != null) {
                for (int j = 0; j < link.size; ++j) {
                    long pointer = link.pointers[j];
                    int bIndex = (int) ((pointer >> 32) & 0xffffffff);
                    int tIndex = (int) (pointer & 0xffffffff);
                    accessorBuild.reset(buffers.get(bIndex));
                    int c = tpComparator.compare(accessorProbe, i, accessorBuild, tIndex);
                    if (c == 0) {
                        matchFound = true;
                        if (!appender.appendConcat(accessorProbe, i, accessorBuild, tIndex)) {
                            flushFrame(outBuffer, writer);
                            appender.reset(outBuffer, true);
                            if (!appender.appendConcat(accessorProbe, i, accessorBuild, tIndex)) {
                                throw new IllegalStateException();
                            }
                        }
                    }
                }
            }
            if (!matchFound && isLeftOuter) {
                if (!appender.appendConcat(accessorProbe, i, nullTupleBuild.getFieldEndOffsets(), nullTupleBuild.getByteArray(), 0, nullTupleBuild.getSize())) {
                    flushFrame(outBuffer, writer);
                    appender.reset(outBuffer, true);
                    if (!appender.appendConcat(accessorProbe, i, nullTupleBuild.getFieldEndOffsets(), nullTupleBuild.getByteArray(), 0, nullTupleBuild
                            .getSize())) {
                        throw new IllegalStateException();
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
        writer.nextFrame(buffer);
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