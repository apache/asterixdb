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
package org.apache.asterix.runtime.operators.joins;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameBufferManager;
import org.apache.hyracks.dataflow.std.sort.FrameSorterMergeSort;
import org.apache.hyracks.dataflow.std.util.FrameTuplePairComparator;

public class InMemoryIntervalPartitionJoin {

    private final IHyracksTaskContext ctx;
    private final List<ByteBuffer> buffers;
    private final FrameTupleAccessor accessorBuild;
    private IFrameTupleAccessor accessorProbe;
    private final FrameTupleAppender appender;
    private final FrameTuplePairComparator tpComparator;
    private final boolean reverseOutputOrder; //Should we reverse the order of tuples, we are writing in output
    private final IPredicateEvaluator predEvaluator;
    private IFrameBufferManager fbm;
    private FrameSorterMergeSort fsms;

    private static final Logger LOGGER = Logger.getLogger(InMemoryIntervalPartitionJoin.class.getName());

    public InMemoryIntervalPartitionJoin(IHyracksTaskContext ctx, IFrameBufferManager fbm,
            FrameTupleAccessor accessorProbe, FrameTupleAccessor accessorBuild, IPredicateEvaluator predEval,
            boolean reverse, int[] buildFields, int[] probeFields, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, int outputLimit) throws HyracksDataException {
        this.ctx = ctx;
        buffers = new ArrayList<ByteBuffer>();
        this.accessorBuild = accessorBuild;
        this.accessorProbe = accessorProbe;
        appender = new FrameTupleAppender(new VSizeFrame(ctx));
        predEvaluator = predEval;
        reverseOutputOrder = reverse;
        this.fbm = fbm;
        IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        tpComparator = new FrameTuplePairComparator(buildFields, probeFields, comparators);

        this.fsms = new FrameSorterMergeSort(ctx, fbm, buildFields, null, comparatorFactories, recordDescriptor,
                outputLimit);
        if (fbm.getNumFrames() != 0) {
            this.fsms.sort();
        }
        LOGGER.fine(
                "InMemoryIntervalPartitionJoin has been created for Thread ID " + Thread.currentThread().getId() + ".");
    }

    public void join(IFrameTupleAccessor accessorProbe, int tid, IFrameWriter writer) throws HyracksDataException {
        this.accessorProbe = accessorProbe;
        if (fbm.getNumFrames() != 0) {
            for (int i = 0; i < fsms.getTuplecount(); ++i) {
                int tIndex = fsms.getTuple(i, accessorBuild);
                int c = tpComparator.compare(accessorProbe, tid, accessorBuild, tIndex);
                if (c == 0) {
                    boolean predEval = evaluatePredicate(tid, tIndex);
                    if (predEval) {
                        appendToResult(tid, tIndex, writer);
                    }
                }
            }
        }
    }

    public void join(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount0 = accessorProbe.getTupleCount();
        for (int i = 0; i < tupleCount0; ++i) {
            join(accessorProbe, i, writer);
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

    private boolean evaluatePredicate(int tIx1, int tIx2) {
        if (reverseOutputOrder) { //Role Reversal Optimization is triggered
            return ((predEvaluator == null) || predEvaluator.evaluate(accessorBuild, tIx2, accessorProbe, tIx1));
        } else {
            return ((predEvaluator == null) || predEvaluator.evaluate(accessorProbe, tIx1, accessorBuild, tIx2));
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
