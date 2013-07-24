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
package edu.uci.ics.hyracks.dataflow.std.join;

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.IPredicateEvaluator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparator;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;

public class NestedLoopJoin {
    private final FrameTupleAccessor accessorInner;
    private final FrameTupleAccessor accessorOuter;
    private final FrameTupleAppender appender;
    private final ITuplePairComparator tpComparator;
    private final ByteBuffer outBuffer;
    private final ByteBuffer innerBuffer;
    private final List<ByteBuffer> outBuffers;
    private final int memSize;
    private final IHyracksTaskContext ctx;
    private RunFileReader runFileReader;
    private int currentMemSize = 0;
    private final RunFileWriter runFileWriter;
    private final boolean isLeftOuter;
    private final ArrayTupleBuilder nullTupleBuilder;
    private final IPredicateEvaluator predEvaluator;
    private boolean isReversed;		//Added for handling correct calling for predicate-evaluator upon recursive calls (in OptimizedHybridHashJoin) that cause role-reversal

    
    public NestedLoopJoin(IHyracksTaskContext ctx, FrameTupleAccessor accessor0, FrameTupleAccessor accessor1,
            ITuplePairComparator comparators, int memSize, IPredicateEvaluator predEval, boolean isLeftOuter, INullWriter[] nullWriters1)
            throws HyracksDataException {
        this.accessorInner = accessor1;
        this.accessorOuter = accessor0;
        this.appender = new FrameTupleAppender(ctx.getFrameSize());
        this.tpComparator = comparators;
        this.outBuffer = ctx.allocateFrame();
        this.innerBuffer = ctx.allocateFrame();
        this.appender.reset(outBuffer, true);
        this.outBuffers = new ArrayList<ByteBuffer>();
        this.memSize = memSize;
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
        for (ByteBuffer outBuffer : outBuffers) {
            runFileReader = runFileWriter.createReader();
            runFileReader.open();
            while (runFileReader.nextFrame(innerBuffer)) {
                blockJoin(outBuffer, innerBuffer, writer);
            }
            runFileReader.close();
        }
        currentMemSize = 0;
        reloadFrame(outerBuffer);
    }

    private void createAndCopyFrame(ByteBuffer outerBuffer) throws HyracksDataException {
        ByteBuffer outerBufferCopy = ctx.allocateFrame();
        FrameUtils.copy(outerBuffer, outerBufferCopy);
        outBuffers.add(outerBufferCopy);
        currentMemSize++;
    }

    private void reloadFrame(ByteBuffer outerBuffer) {
        outBuffers.get(currentMemSize).clear();
        FrameUtils.copy(outerBuffer, outBuffers.get(currentMemSize));
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
                    if (!appender.appendConcat(accessorOuter, i, accessorInner, j)) {
                        flushFrame(outBuffer, writer);
                        appender.reset(outBuffer, true);
                        if (!appender.appendConcat(accessorOuter, i, accessorInner, j)) {
                            int tSize = accessorOuter.getTupleEndOffset(i) - accessorOuter.getTupleStartOffset(i)
                                    + accessorInner.getTupleEndOffset(j) - accessorInner.getTupleStartOffset(j);
                            throw new HyracksDataException("Record size (" + tSize + ") larger than frame size ("
                                    + appender.getBuffer().capacity() + ")");
                        }
                    }
                }
            }

            if (!matchFound && isLeftOuter) {
                if (!appender.appendConcat(accessorOuter, i, nullTupleBuilder.getFieldEndOffsets(),
                        nullTupleBuilder.getByteArray(), 0, nullTupleBuilder.getSize())) {
                    flushFrame(outBuffer, writer);
                    appender.reset(outBuffer, true);
                    if (!appender.appendConcat(accessorOuter, i, nullTupleBuilder.getFieldEndOffsets(),
                            nullTupleBuilder.getByteArray(), 0, nullTupleBuilder.getSize())) {
                        int tSize = accessorOuter.getTupleEndOffset(i) - accessorOuter.getTupleStartOffset(i)
                                + nullTupleBuilder.getSize();
                        throw new HyracksDataException("Record size (" + tSize + ") larger than frame size ("
                                + appender.getBuffer().capacity() + ")");
                    }
                }
            }
        }
    }
    
    private boolean evaluatePredicate(int tIx1, int tIx2){
    	if(isReversed){		//Role Reversal Optimization is triggered
    		return ( (predEvaluator == null) || predEvaluator.evaluate(accessorInner, tIx2, accessorOuter, tIx1) );
    	}
    	else {
    		return ( (predEvaluator == null) || predEvaluator.evaluate(accessorOuter, tIx1, accessorInner, tIx2) );
    	}
    }

    public void closeCache() throws HyracksDataException {
        if (runFileWriter != null) {
            runFileWriter.close();
        }
    }

    public void closeJoin(IFrameWriter writer) throws HyracksDataException {
        for (int i = 0; i < currentMemSize; i++) {
            ByteBuffer outBuffer = outBuffers.get(i);
            runFileReader = runFileWriter.createReader();
            runFileReader.open();
            while (runFileReader.nextFrame(innerBuffer)) {
                blockJoin(outBuffer, innerBuffer, writer);
            }
            runFileReader.close();
        }
        outBuffers.clear();
        currentMemSize = 0;

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

    private int compare(FrameTupleAccessor accessor0, int tIndex0, FrameTupleAccessor accessor1, int tIndex1)
            throws HyracksDataException {
        int c = tpComparator.compare(accessor0, tIndex0, accessor1, tIndex1);
        if (c != 0) {
            return c;
        }
        return 0;
    }
    
    public void setIsReversed(boolean b){
    	this.isReversed = b;
    }
}
