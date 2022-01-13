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
import java.util.BitSet;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.exceptions.ErrorCode;
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
    // Note: Min memory budget should be less than {@code AbstractJoinPOperator.MIN_FRAME_LIMIT_FOR_JOIN}
    // Inner join: 1 frame for the outer input side, 1 frame for the inner input side, 1 frame for the output
    private static final int MIN_FRAME_BUDGET_INNER_JOIN = 3;
    // Outer join extra: Add 1 frame for the {@code outerMatchLOJ} bitset
    private static final int MIN_FRAME_BUDGET_OUTER_JOIN = MIN_FRAME_BUDGET_INNER_JOIN + 1;
    // Outer join needs 1 bit per each tuple in the outer side buffer
    private static final int ESTIMATE_AVG_TUPLE_SIZE = 128;

    private final FrameTupleAccessor accessorInner;
    private final FrameTupleAccessor accessorOuter;
    private final FrameTupleAppender appender;
    private ITuplePairComparator tpComparator;
    private final IFrame outBuffer;
    private final IFrame innerBuffer;
    private final VariableFrameMemoryManager outerBufferMngr;
    private final RunFileWriter runFileWriter;
    private final boolean isLeftOuter;
    private final ArrayTupleBuilder missingTupleBuilder;
    // Added for handling correct calling of recursive calls
    // (in OptimizedHybridHashJoin) that cause role-reversal
    private final boolean isReversed;
    private final BufferInfo tempInfo = new BufferInfo(null, -1, -1);
    private final BitSet outerMatchLOJ;

    public NestedLoopJoin(IHyracksJobletContext jobletContext, FrameTupleAccessor accessorOuter,
            FrameTupleAccessor accessorInner, int memBudgetInFrames, boolean isLeftOuter,
            IMissingWriter[] missingWriters) throws HyracksDataException {
        this(jobletContext, accessorOuter, accessorInner, memBudgetInFrames, isLeftOuter, missingWriters, false);
    }

    public NestedLoopJoin(IHyracksJobletContext jobletContext, FrameTupleAccessor accessorOuter,
            FrameTupleAccessor accessorInner, int memBudgetInFrames, boolean isLeftOuter,
            IMissingWriter[] missingWriters, boolean isReversed) throws HyracksDataException {
        this.accessorInner = accessorInner;
        this.accessorOuter = accessorOuter;
        this.appender = new FrameTupleAppender();
        this.outBuffer = new VSizeFrame(jobletContext);
        this.innerBuffer = new VSizeFrame(jobletContext);
        this.appender.reset(outBuffer, true);

        int minMemBudgetInFrames = isLeftOuter ? MIN_FRAME_BUDGET_OUTER_JOIN : MIN_FRAME_BUDGET_INNER_JOIN;
        if (memBudgetInFrames < minMemBudgetInFrames) {
            throw new HyracksDataException(ErrorCode.INSUFFICIENT_MEMORY);
        }
        int outerBufferMngrMemBudgetInFrames = memBudgetInFrames - minMemBudgetInFrames + 1;
        int outerBufferMngrMemBudgetInBytes = jobletContext.getInitialFrameSize() * outerBufferMngrMemBudgetInFrames;
        this.outerBufferMngr = new VariableFrameMemoryManager(
                new VariableFramePool(jobletContext, outerBufferMngrMemBudgetInBytes), FrameFreeSlotPolicyFactory
                        .createFreeSlotPolicy(EnumFreeSlotPolicy.LAST_FIT, outerBufferMngrMemBudgetInFrames));

        this.isLeftOuter = isLeftOuter;
        if (isLeftOuter) {
            if (isReversed) {
                throw new HyracksDataException(ErrorCode.ILLEGAL_STATE, "Outer join cannot reverse roles");
            }
            int innerFieldCount = this.accessorInner.getFieldCount();
            missingTupleBuilder = new ArrayTupleBuilder(innerFieldCount);
            DataOutput out = missingTupleBuilder.getDataOutput();
            for (int i = 0; i < innerFieldCount; i++) {
                missingWriters[i].writeMissing(out);
                missingTupleBuilder.addFieldEndOffset();
            }
            // Outer join needs 1 bit per each tuple in the outer side buffer
            int outerMatchLOJCardinalityEstimate = outerBufferMngrMemBudgetInBytes / ESTIMATE_AVG_TUPLE_SIZE;
            outerMatchLOJ = new BitSet(Math.max(outerMatchLOJCardinalityEstimate, 1));
        } else {
            missingTupleBuilder = null;
            outerMatchLOJ = null;
        }
        this.isReversed = isReversed;

        FileReference file =
                jobletContext.createManagedWorkspaceFile(this.getClass().getSimpleName() + this.toString());
        runFileWriter = new RunFileWriter(file, jobletContext.getIoManager());
        runFileWriter.open();
    }

    public void cache(ByteBuffer buffer) throws HyracksDataException {
        accessorInner.reset(buffer);
        if (accessorInner.getTupleCount() > 0) {
            runFileWriter.nextFrame(buffer);
        }
    }

    /**
     * Must be called before starting to join to set the right comparator with the right context.
     *
     * @param comparator the comparator to use for comparing the probe tuples against the build tuples
     */
    void setComparator(ITuplePairComparator comparator) {
        tpComparator = comparator;
    }

    public void join(ByteBuffer outerBuffer, IFrameWriter writer) throws HyracksDataException {
        accessorOuter.reset(outerBuffer);
        if (accessorOuter.getTupleCount() <= 0) {
            return;
        }
        if (outerBufferMngr.insertFrame(outerBuffer) < 0) {
            multiBlockJoin(writer);
            outerBufferMngr.reset();
            if (outerBufferMngr.insertFrame(outerBuffer) < 0) {
                throw new HyracksDataException("The given outer frame of size:" + outerBuffer.capacity()
                        + " is too big to cache in the buffer. Please choose a larger buffer memory size");
            }
        }
    }

    private void multiBlockJoin(IFrameWriter writer) throws HyracksDataException {
        int outerBufferFrameCount = outerBufferMngr.getNumFrames();
        if (outerBufferFrameCount == 0) {
            return;
        }
        RunFileReader runFileReader = runFileWriter.createReader();
        try {
            runFileReader.open();
            if (isLeftOuter) {
                outerMatchLOJ.clear();
            }
            while (runFileReader.nextFrame(innerBuffer)) {
                int outerTupleRunningCount = 0;
                for (int i = 0; i < outerBufferFrameCount; i++) {
                    BufferInfo outerBufferInfo = outerBufferMngr.getFrame(i, tempInfo);
                    accessorOuter.reset(outerBufferInfo.getBuffer(), outerBufferInfo.getStartOffset(),
                            outerBufferInfo.getLength());
                    int outerTupleCount = accessorOuter.getTupleCount();
                    accessorInner.reset(innerBuffer.getBuffer());
                    blockJoin(outerTupleRunningCount, writer);
                    outerTupleRunningCount += outerTupleCount;
                }
            }
            if (isLeftOuter) {
                int outerTupleRunningCount = 0;
                for (int i = 0; i < outerBufferFrameCount; i++) {
                    BufferInfo outerBufferInfo = outerBufferMngr.getFrame(i, tempInfo);
                    accessorOuter.reset(outerBufferInfo.getBuffer(), outerBufferInfo.getStartOffset(),
                            outerBufferInfo.getLength());
                    int outerFrameTupleCount = accessorOuter.getTupleCount();
                    appendMissing(outerTupleRunningCount, outerFrameTupleCount, writer);
                    outerTupleRunningCount += outerFrameTupleCount;
                }
            }
        } finally {
            runFileReader.close();
        }
    }

    private void blockJoin(int outerTupleStartPos, IFrameWriter writer) throws HyracksDataException {
        int outerTupleCount = accessorOuter.getTupleCount();
        int innerTupleCount = accessorInner.getTupleCount();
        for (int i = 0; i < outerTupleCount; ++i) {
            boolean matchFound = false;
            for (int j = 0; j < innerTupleCount; ++j) {
                int c = tpComparator.compare(accessorOuter, i, accessorInner, j);
                if (c == 0) {
                    matchFound = true;
                    appendToResults(i, j, writer);
                }
            }
            if (isLeftOuter && matchFound) {
                outerMatchLOJ.set(outerTupleStartPos + i);
            }
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

    private void appendMissing(int outerFrameMngrStartPos, int outerFrameTupleCount, IFrameWriter writer)
            throws HyracksDataException {
        int limit = outerFrameMngrStartPos + outerFrameTupleCount;
        for (int outerTuplePos =
                outerMatchLOJ.nextClearBit(outerFrameMngrStartPos); outerTuplePos < limit; outerTuplePos =
                        outerMatchLOJ.nextClearBit(outerTuplePos + 1)) {
            int[] ntFieldEndOffsets = missingTupleBuilder.getFieldEndOffsets();
            byte[] ntByteArray = missingTupleBuilder.getByteArray();
            int ntSize = missingTupleBuilder.getSize();
            int outerAccessorTupleIndex = outerTuplePos - outerFrameMngrStartPos;
            FrameUtils.appendConcatToWriter(writer, appender, accessorOuter, outerAccessorTupleIndex, ntFieldEndOffsets,
                    ntByteArray, 0, ntSize);
        }
    }

    public void closeCache() throws HyracksDataException {
        if (runFileWriter != null) {
            runFileWriter.close();
        }
    }

    public void completeJoin(IFrameWriter writer) throws HyracksDataException {
        try {
            multiBlockJoin(writer);
        } finally {
            runFileWriter.eraseClosed();
        }
        appender.write(writer, true);
    }

    public void releaseMemory() throws HyracksDataException {
        outerBufferMngr.reset();
    }
}
