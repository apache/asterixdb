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

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.INullWriter;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;

public class NestedLoopJoin {
    private final FrameTupleAccessor accessorInner;
    private final FrameTupleAccessor accessorOuter;
    private final FrameTupleAppender appender;
    private final ITuplePairComparator tpComparator;
    private final IFrame outBuffer;
    private final IFrame innerBuffer;
    private final List<ByteBuffer> outBuffers;
    private final int memSize;
    private final IHyracksTaskContext ctx;
    private RunFileReader runFileReader;
    private int currentMemSize = 0;
    private final RunFileWriter runFileWriter;
    private final boolean isLeftOuter;
    private final ArrayTupleBuilder nullTupleBuilder;
    private final IPredicateEvaluator predEvaluator;
    private boolean isReversed;        //Added for handling correct calling for predicate-evaluator upon recursive calls (in OptimizedHybridHashJoin) that cause role-reversal

    public NestedLoopJoin(IHyracksTaskContext ctx, FrameTupleAccessor accessor0, FrameTupleAccessor accessor1,
            ITuplePairComparator comparators, int memSize, IPredicateEvaluator predEval, boolean isLeftOuter,
            INullWriter[] nullWriters1)
            throws HyracksDataException {
        this.accessorInner = accessor1;
        this.accessorOuter = accessor0;
        this.appender = new FrameTupleAppender();
        this.tpComparator = comparators;
        this.outBuffer = new VSizeFrame(ctx);
        this.innerBuffer = new VSizeFrame(ctx);
        this.appender.reset(outBuffer, true);
        this.outBuffers = new ArrayList<ByteBuffer>();
        this.memSize = memSize;
        if (memSize < 3) {
            throw new HyracksDataException("Not enough memory is available for Nested Loop Join");
        }
        this.predEvaluator = predEval;
        this.isReversed = false;
        this.ctx = ctx;

        this.isLeftOuter = isLeftOuter;
        if (isLeftOuter) {
            int innerFieldCount = accessorInner.getFieldCount();
            nullTupleBuilder = new ArrayTupleBuilder(innerFieldCount);
            DataOutput out = nullTupleBuilder.getDataOutput();
            for (int i = 0; i < innerFieldCount; i++) {
                nullWriters1[i].writeNull(out);
                nullTupleBuilder.addFieldEndOffset();
            }
        } else {
            nullTupleBuilder = null;
        }

        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                this.getClass().getSimpleName() + this.toString());
        runFileWriter = new RunFileWriter(file, ctx.getIOManager());
        runFileWriter.open();
    }

    public void cache(ByteBuffer buffer) throws HyracksDataException {
        runFileWriter.nextFrame(buffer);
    }

    public void join(ByteBuffer outerBuffer, IFrameWriter writer) throws HyracksDataException {
        if (outBuffers.size() < memSize - 3) {
            createAndCopyFrame(outerBuffer);
            return;
        }
        if (currentMemSize < memSize - 3) {
            reloadFrame(outerBuffer);
            return;
        }
        runFileReader = runFileWriter.createReader();
        runFileReader.open();
        while (runFileReader.nextFrame(innerBuffer)) {
            for (ByteBuffer outBuffer : outBuffers) {
                blockJoin(outBuffer, innerBuffer.getBuffer(), writer);
            }
        }
        runFileReader.close();
        currentMemSize = 0;
        reloadFrame(outerBuffer);
    }

    private void createAndCopyFrame(ByteBuffer outerBuffer) throws HyracksDataException {
        ByteBuffer outerBufferCopy = ctx.allocateFrame(outerBuffer.capacity());
        FrameUtils.copyAndFlip(outerBuffer, outerBufferCopy);
        outBuffers.add(outerBufferCopy);
        currentMemSize++;
    }

    private void reloadFrame(ByteBuffer outerBuffer) throws HyracksDataException {
        outBuffers.get(currentMemSize).clear();
        if (outBuffers.get(currentMemSize).capacity() != outerBuffer.capacity()) {
            outBuffers.set(currentMemSize, ctx.allocateFrame(outerBuffer.capacity()));
        }
        FrameUtils.copyAndFlip(outerBuffer, outBuffers.get(currentMemSize));
        currentMemSize++;
    }

    private void blockJoin(ByteBuffer outerBuffer, ByteBuffer innerBuffer, IFrameWriter writer)
            throws HyracksDataException {
        accessorOuter.reset(outerBuffer);
        accessorInner.reset(innerBuffer);
        int tupleCount0 = accessorOuter.getTupleCount();
        int tupleCount1 = accessorInner.getTupleCount();

        for (int i = 0; i < tupleCount0; ++i) {
            boolean matchFound = false;
            for (int j = 0; j < tupleCount1; ++j) {
                int c = compare(accessorOuter, i, accessorInner, j);
                boolean prdEval = evaluatePredicate(i, j);
                if (c == 0 && prdEval) {
                    matchFound = true;
                    appendToResults(i, j, writer);
                }
            }

            if (!matchFound && isLeftOuter) {
                final int[] ntFieldEndOffsets = nullTupleBuilder.getFieldEndOffsets();
                final byte[] ntByteArray = nullTupleBuilder.getByteArray();
                final int ntSize = nullTupleBuilder.getSize();
                FrameUtils.appendConcatToWriter(writer, appender, accessorOuter, i, ntFieldEndOffsets, ntByteArray, 0,
                        ntSize);
            }
        }
    }

    private boolean evaluatePredicate(int tIx1, int tIx2) {
        if (isReversed) {        //Role Reversal Optimization is triggered
            return ((predEvaluator == null) || predEvaluator.evaluate(accessorInner, tIx2, accessorOuter, tIx1));
        } else {
            return ((predEvaluator == null) || predEvaluator.evaluate(accessorOuter, tIx1, accessorInner, tIx2));
        }
    }

    private void appendToResults(int outerTupleId, int innerTupleId, IFrameWriter writer) throws HyracksDataException {
        if (!isReversed) {
            appendResultToFrame(accessorOuter, outerTupleId, accessorInner, innerTupleId, writer);
        } else {
            //Role Reversal Optimization is triggered
            appendResultToFrame(accessorInner, innerTupleId, accessorOuter, outerTupleId, writer);
        }
    }

    private void appendResultToFrame(FrameTupleAccessor accessor1, int tupleId1, FrameTupleAccessor accessor2,
            int tupleId2, IFrameWriter writer) throws HyracksDataException {
        FrameUtils.appendConcatToWriter(writer, appender, accessor1, tupleId1, accessor2, tupleId2);
    }

    public void closeCache() throws HyracksDataException {
        if (runFileWriter != null) {
            runFileWriter.close();
        }
    }

    public void closeJoin(IFrameWriter writer) throws HyracksDataException {
        runFileReader = runFileWriter.createReader();
        runFileReader.open();
        while (runFileReader.nextFrame(innerBuffer)) {
            for (int i = 0; i < currentMemSize; i++) {
                blockJoin(outBuffers.get(i), innerBuffer.getBuffer(), writer);
            }
        }
        runFileReader.close();
        outBuffers.clear();
        currentMemSize = 0;

        appender.flush(writer, true);
    }

    private int compare(FrameTupleAccessor accessor0, int tIndex0, FrameTupleAccessor accessor1, int tIndex1)
            throws HyracksDataException {
        int c = tpComparator.compare(accessor0, tIndex0, accessor1, tIndex1);
        if (c != 0) {
            return c;
        }
        return 0;
    }

    public void setIsReversed(boolean b) {
        this.isReversed = b;
    }
}
