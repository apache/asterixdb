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

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
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
import org.apache.hyracks.dataflow.std.buffermanager.BufferInfo;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.buffermanager.FrameFreeSlotPolicyFactory;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFrameMemoryManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFramePool;

public class NestedLoopJoin {
    private final FrameTupleAccessor accessorInner;
    private final FrameTupleAccessor accessorOuter;
    private final FrameTupleAppender appender;
    private final ITuplePairComparator tpComparator;
    private final IFrame outBuffer;
    private final IFrame innerBuffer;
    private final VariableFrameMemoryManager outerBufferMngr;
    private final RunFileWriter runFileWriter;
    private final boolean isLeftOuter;
    private final ArrayTupleBuilder missingTupleBuilder;
    private final IPredicateEvaluator predEvaluator;
    private boolean isReversed; //Added for handling correct calling for predicate-evaluator upon recursive calls (in OptimizedHybridHashJoin) that cause role-reversal
    private BufferInfo tempInfo = new BufferInfo(null, -1, -1);

    public NestedLoopJoin(IHyracksTaskContext ctx, FrameTupleAccessor accessorOuter, FrameTupleAccessor accessorInner,
            ITuplePairComparator comparatorsOuter2Inner, int memSize, IPredicateEvaluator predEval, boolean isLeftOuter,
            IMissingWriter[] missingWriters) throws HyracksDataException {
        this.accessorInner = accessorInner;
        this.accessorOuter = accessorOuter;
        this.appender = new FrameTupleAppender();
        this.tpComparator = comparatorsOuter2Inner;
        this.outBuffer = new VSizeFrame(ctx);
        this.innerBuffer = new VSizeFrame(ctx);
        this.appender.reset(outBuffer, true);
        if (memSize < 3) {
            throw new HyracksDataException("Not enough memory is available for Nested Loop Join");
        }
        this.outerBufferMngr =
                new VariableFrameMemoryManager(new VariableFramePool(ctx, ctx.getInitialFrameSize() * (memSize - 2)),
                        FrameFreeSlotPolicyFactory.createFreeSlotPolicy(EnumFreeSlotPolicy.LAST_FIT, memSize - 2));

        this.predEvaluator = predEval;
        this.isReversed = false;

        this.isLeftOuter = isLeftOuter;
        if (isLeftOuter) {
            int innerFieldCount = this.accessorInner.getFieldCount();
            missingTupleBuilder = new ArrayTupleBuilder(innerFieldCount);
            DataOutput out = missingTupleBuilder.getDataOutput();
            for (int i = 0; i < innerFieldCount; i++) {
                missingWriters[i].writeMissing(out);
                missingTupleBuilder.addFieldEndOffset();
            }
        } else {
            missingTupleBuilder = null;
        }

        FileReference file =
                ctx.getJobletContext().createManagedWorkspaceFile(this.getClass().getSimpleName() + this.toString());
        runFileWriter = new RunFileWriter(file, ctx.getIoManager());
        runFileWriter.open();
    }

    public void cache(ByteBuffer buffer) throws HyracksDataException {
        runFileWriter.nextFrame(buffer);
    }

    public void join(ByteBuffer outerBuffer, IFrameWriter writer) throws HyracksDataException {
        if (outerBufferMngr.insertFrame(outerBuffer) < 0) {
            RunFileReader runFileReader = runFileWriter.createReader();
            try {
                runFileReader.open();
                while (runFileReader.nextFrame(innerBuffer)) {
                    for (int i = 0; i < outerBufferMngr.getNumFrames(); i++) {
                        blockJoin(outerBufferMngr.getFrame(i, tempInfo), innerBuffer.getBuffer(), writer);
                    }
                }
            } finally {
                runFileReader.close();
            }
            outerBufferMngr.reset();
            if (outerBufferMngr.insertFrame(outerBuffer) < 0) {
                throw new HyracksDataException("The given outer frame of size:" + outerBuffer.capacity()
                        + " is too big to cache in the buffer. Please choose a larger buffer memory size");
            }
        }
    }

    private void blockJoin(BufferInfo outerBufferInfo, ByteBuffer innerBuffer, IFrameWriter writer)
            throws HyracksDataException {
        accessorOuter.reset(outerBufferInfo.getBuffer(), outerBufferInfo.getStartOffset(), outerBufferInfo.getLength());
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
                final int[] ntFieldEndOffsets = missingTupleBuilder.getFieldEndOffsets();
                final byte[] ntByteArray = missingTupleBuilder.getByteArray();
                final int ntSize = missingTupleBuilder.getSize();
                FrameUtils.appendConcatToWriter(writer, appender, accessorOuter, i, ntFieldEndOffsets, ntByteArray, 0,
                        ntSize);
            }
        }
    }

    private boolean evaluatePredicate(int tIx1, int tIx2) {
        if (isReversed) { //Role Reversal Optimization is triggered
            return ((predEvaluator == null) || predEvaluator.evaluate(accessorInner, tIx2, accessorOuter, tIx1));
        } else {
            return ((predEvaluator == null) || predEvaluator.evaluate(accessorOuter, tIx1, accessorInner, tIx2));
        }
    }

    private void appendToResults(int outerTupleId, int innerTupleId, IFrameWriter writer) throws HyracksDataException {
        if (isReversed) {
            appendResultToFrame(accessorInner, innerTupleId, accessorOuter, outerTupleId, writer);
        } else {
            appendResultToFrame(accessorOuter, outerTupleId, accessorInner, innerTupleId, writer);
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

    public void completeJoin(IFrameWriter writer) throws HyracksDataException {
        RunFileReader runFileReader = runFileWriter.createDeleteOnCloseReader();
        try {
            runFileReader.open();
            while (runFileReader.nextFrame(innerBuffer)) {
                for (int i = 0; i < outerBufferMngr.getNumFrames(); i++) {
                    blockJoin(outerBufferMngr.getFrame(i, tempInfo), innerBuffer.getBuffer(), writer);
                }
            }
        } finally {
            runFileReader.close();
        }
        appender.write(writer, true);
    }

    public void releaseMemory() throws HyracksDataException {
        outerBufferMngr.reset();
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
