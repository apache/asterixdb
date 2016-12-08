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

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.hyracks.dataflow.std.util.FrameTuplePairComparator;

public class InMemoryHashJoin {

    private final IHyracksTaskContext ctx;
    private final List<ByteBuffer> buffers;
    private final FrameTupleAccessor accessorBuild;
    private final ITuplePartitionComputer tpcBuild;
    private IFrameTupleAccessor accessorProbe;
    private final ITuplePartitionComputer tpcProbe;
    private final FrameTupleAppender appender;
    private final FrameTuplePairComparator tpComparator;
    private final boolean isLeftOuter;
    private final ArrayTupleBuilder missingTupleBuild;
    private final ISerializableTable table;
    private final int tableSize;
    private final TuplePointer storedTuplePointer;
    private final boolean reverseOutputOrder; //Should we reverse the order of tuples, we are writing in output
    private final IPredicateEvaluator predEvaluator;
    private final boolean isTableCapacityNotZero;

    private static final Logger LOGGER = Logger.getLogger(InMemoryHashJoin.class.getName());

    public InMemoryHashJoin(IHyracksTaskContext ctx, int tableSize, FrameTupleAccessor accessorProbe,
            ITuplePartitionComputer tpcProbe, FrameTupleAccessor accessorBuild, ITuplePartitionComputer tpcBuild,
            FrameTuplePairComparator comparator, boolean isLeftOuter, IMissingWriter[] missingWritersBuild,
            ISerializableTable table, IPredicateEvaluator predEval) throws HyracksDataException {
        this(ctx, tableSize, accessorProbe, tpcProbe, accessorBuild, tpcBuild, comparator, isLeftOuter,
                missingWritersBuild, table, predEval, false);
    }

    public InMemoryHashJoin(IHyracksTaskContext ctx, int tableSize, FrameTupleAccessor accessorProbe,
            ITuplePartitionComputer tpcProbe, FrameTupleAccessor accessorBuild, ITuplePartitionComputer tpcBuild,
            FrameTuplePairComparator comparator, boolean isLeftOuter, IMissingWriter[] missingWritersBuild,
            ISerializableTable table, IPredicateEvaluator predEval, boolean reverse) throws HyracksDataException {
        this.ctx = ctx;
        this.tableSize = tableSize;
        this.table = table;
        storedTuplePointer = new TuplePointer();
        buffers = new ArrayList<>();
        this.accessorBuild = accessorBuild;
        this.tpcBuild = tpcBuild;
        this.accessorProbe = accessorProbe;
        this.tpcProbe = tpcProbe;
        appender = new FrameTupleAppender(new VSizeFrame(ctx));
        tpComparator = comparator;
        predEvaluator = predEval;
        this.isLeftOuter = isLeftOuter;
        if (isLeftOuter) {
            int fieldCountOuter = accessorBuild.getFieldCount();
            missingTupleBuild = new ArrayTupleBuilder(fieldCountOuter);
            DataOutput out = missingTupleBuild.getDataOutput();
            for (int i = 0; i < fieldCountOuter; i++) {
                missingWritersBuild[i].writeMissing(out);
                missingTupleBuild.addFieldEndOffset();
            }
        } else {
            missingTupleBuild = null;
        }
        reverseOutputOrder = reverse;
        if (tableSize != 0) {
            isTableCapacityNotZero = true;
        } else {
            isTableCapacityNotZero = false;
        }
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
            storedTuplePointer.reset(bIndex, i);
            table.insert(entry, storedTuplePointer);
        }
    }

    /**
     * Reads the given tuple from the probe side and joins it with tuples from the build side.
     * This method assumes that the accessorProbe is already set to the current probe frame.
     */
    void join(int tid, IFrameWriter writer) throws HyracksDataException {
        boolean matchFound = false;
        if (isTableCapacityNotZero) {
            int entry = tpcProbe.partition(accessorProbe, tid, tableSize);
            int tupleCount = table.getTupleCount(entry);
            for (int i = 0; i < tupleCount; i++) {
                table.getTuplePointer(entry, i, storedTuplePointer);
                int bIndex = storedTuplePointer.getFrameIndex();
                int tIndex = storedTuplePointer.getTupleIndex();
                accessorBuild.reset(buffers.get(bIndex));
                int c = tpComparator.compare(accessorProbe, tid, accessorBuild, tIndex);
                if (c == 0) {
                    boolean predEval = evaluatePredicate(tid, tIndex);
                    if (predEval) {
                        matchFound = true;
                        appendToResult(tid, tIndex, writer);
                    }
                }
            }
        }
        if (!matchFound && isLeftOuter) {
            FrameUtils.appendConcatToWriter(writer, appender, accessorProbe, tid,
                    missingTupleBuild.getFieldEndOffsets(), missingTupleBuild.getByteArray(), 0,
                    missingTupleBuild.getSize());
        }
    }

    public void join(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount0 = accessorProbe.getTupleCount();
        for (int i = 0; i < tupleCount0; ++i) {
            join(i, writer);
        }
    }

    public void resetAccessorProbe(IFrameTupleAccessor newAccessorProbe) {
        accessorProbe.reset(newAccessorProbe.getBuffer());
    }

    public void closeJoin(IFrameWriter writer) throws HyracksDataException {
        appender.write(writer, true);
        int nFrames = buffers.size();
        int totalSize = 0;
        for (int i = 0; i < nFrames; i++) {
            totalSize += buffers.get(i).capacity();
        }
        buffers.clear();
        ctx.deallocateFrames(totalSize);
        LOGGER.fine("InMemoryHashJoin has finished using " + nFrames + " frames for Thread ID "
                + Thread.currentThread().getId() + ".");
    }

    public void closeTable() throws HyracksDataException {
        table.close();
    }

    private boolean evaluatePredicate(int tIx1, int tIx2) {
        if (reverseOutputOrder) { //Role Reversal Optimization is triggered
            return (predEvaluator == null) || predEvaluator.evaluate(accessorBuild, tIx2, accessorProbe, tIx1);
        } else {
            return (predEvaluator == null) || predEvaluator.evaluate(accessorProbe, tIx1, accessorBuild, tIx2);
        }
    }

    private void appendToResult(int probeSidetIx, int buildSidetIx, IFrameWriter writer) throws HyracksDataException {
        if (reverseOutputOrder) {
            FrameUtils.appendConcatToWriter(writer, appender, accessorBuild, buildSidetIx, accessorProbe, probeSidetIx);
        } else {
            FrameUtils.appendConcatToWriter(writer, appender, accessorProbe, probeSidetIx, accessorBuild, buildSidetIx);
        }
    }
}
