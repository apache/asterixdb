/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.dataflow.std.join;

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.INullWriter;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class InMemoryHashJoin {

    private final IHyracksTaskContext ctx;
    private final List<ByteBuffer> buffers;
    private final FrameTupleAccessor accessorBuild;
    private final ITuplePartitionComputer tpcBuild;
    private final FrameTupleAccessor accessorProbe;
    private final ITuplePartitionComputer tpcProbe;
    private final FrameTupleAppender appender;
    private final FrameTuplePairComparator tpComparator;
    private final boolean isLeftOuter;
    private final ArrayTupleBuilder nullTupleBuild;
    private final ISerializableTable table;
    private final int tableSize;
    private final TuplePointer storedTuplePointer;
    private final boolean reverseOutputOrder; //Should we reverse the order of tuples, we are writing in output
    private final IPredicateEvaluator predEvaluator;

    private static final Logger LOGGER = Logger.getLogger(InMemoryHashJoin.class.getName());

    public InMemoryHashJoin(IHyracksTaskContext ctx, int tableSize, FrameTupleAccessor accessor0,
            ITuplePartitionComputer tpc0, FrameTupleAccessor accessor1, ITuplePartitionComputer tpc1,
            FrameTuplePairComparator comparator, boolean isLeftOuter, INullWriter[] nullWriters1,
            ISerializableTable table, IPredicateEvaluator predEval) throws HyracksDataException {
        this(ctx, tableSize, accessor0, tpc0, accessor1, tpc1, comparator, isLeftOuter, nullWriters1, table, predEval,
                false);
    }

    public InMemoryHashJoin(IHyracksTaskContext ctx, int tableSize, FrameTupleAccessor accessor0,
            ITuplePartitionComputer tpc0, FrameTupleAccessor accessor1, ITuplePartitionComputer tpc1,
            FrameTuplePairComparator comparator, boolean isLeftOuter, INullWriter[] nullWriters1,
            ISerializableTable table, IPredicateEvaluator predEval, boolean reverse) throws HyracksDataException {
        this.ctx = ctx;
        this.tableSize = tableSize;
        this.table = table;
        storedTuplePointer = new TuplePointer();
        buffers = new ArrayList<ByteBuffer>();
        this.accessorBuild = accessor1;
        this.tpcBuild = tpc1;
        this.accessorProbe = accessor0;
        this.tpcProbe = tpc0;
        appender = new FrameTupleAppender(new VSizeFrame(ctx));
        tpComparator = comparator;
        predEvaluator = predEval;
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
        reverseOutputOrder = reverse;
        LOGGER.fine("InMemoryHashJoin has been created for a table size of " + tableSize + " for Thread ID "
                + Thread.currentThread().getId() + ".");
    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        buffers.add(buffer);
        int bIndex = buffers.size() - 1;
        accessorBuild.reset(buffer);
        int tCount = accessorBuild.getTupleCount();
        for (int i = 0; i < tCount; ++i) {
            int entry = tpcBuild.partition(accessorBuild, i, tableSize);
            storedTuplePointer.frameIndex = bIndex;
            storedTuplePointer.tupleIndex = i;
            table.insert(entry, storedTuplePointer);
        }
    }

    public void join(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount0 = accessorProbe.getTupleCount();
        for (int i = 0; i < tupleCount0; ++i) {
            boolean matchFound = false;
            if (tableSize != 0) {
                int entry = tpcProbe.partition(accessorProbe, i, tableSize);
                int offset = 0;
                do {
                    table.getTuplePointer(entry, offset++, storedTuplePointer);
                    if (storedTuplePointer.frameIndex < 0)
                        break;
                    int bIndex = storedTuplePointer.frameIndex;
                    int tIndex = storedTuplePointer.tupleIndex;
                    accessorBuild.reset(buffers.get(bIndex));
                    int c = tpComparator.compare(accessorProbe, i, accessorBuild, tIndex);
                    if (c == 0) {
                        boolean predEval = evaluatePredicate(i, tIndex);
                        if (predEval) {
                            matchFound = true;
                            appendToResult(i, tIndex, writer);
                        }
                    }
                } while (true);
            }
            if (!matchFound && isLeftOuter) {
                FrameUtils.appendConcatToWriter(writer, appender, accessorProbe, i,
                        nullTupleBuild.getFieldEndOffsets(), nullTupleBuild.getByteArray(), 0,
                        nullTupleBuild.getSize());
            }
        }
    }

    public void closeJoin(IFrameWriter writer) throws HyracksDataException {
        appender.flush(writer, true);
        int nFrames = buffers.size();
        buffers.clear();
        ctx.deallocateFrames(nFrames);
        LOGGER.fine("InMemoryHashJoin has finished using " + nFrames + " frames for Thread ID "
                + Thread.currentThread().getId() + ".");
    }

    private void flushFrame(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        buffer.position(0);
        buffer.limit(buffer.capacity());
        writer.nextFrame(buffer);
        buffer.position(0);
        buffer.limit(buffer.capacity());
    }

    private boolean evaluatePredicate(int tIx1, int tIx2) {
        if (reverseOutputOrder) { //Role Reversal Optimization is triggered
            return ((predEvaluator == null) || predEvaluator.evaluate(accessorBuild, tIx2, accessorProbe, tIx1));
        } else {
            return ((predEvaluator == null) || predEvaluator.evaluate(accessorProbe, tIx1, accessorBuild, tIx2));
        }
    }

    private void appendToResult(int probeSidetIx, int buildSidetIx, IFrameWriter writer) throws HyracksDataException {
        if (!reverseOutputOrder) {
            FrameUtils.appendConcatToWriter(writer, appender, accessorProbe, probeSidetIx, accessorBuild,
                    buildSidetIx);
        } else {
            FrameUtils.appendConcatToWriter(writer, appender, accessorBuild, buildSidetIx, accessorProbe,
                    probeSidetIx);
        }
    }
}
