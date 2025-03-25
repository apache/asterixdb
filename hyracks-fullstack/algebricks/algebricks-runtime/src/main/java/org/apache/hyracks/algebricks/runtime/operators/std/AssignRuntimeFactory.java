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
package org.apache.hyracks.algebricks.runtime.operators.std;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public class AssignRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;
    protected int[] outColumns;
    protected IScalarEvaluatorFactory[] evalFactories;
    protected final boolean flushFramesRapidly;

    /**
     * @param outColumns
     *            a sorted array of columns into which the result is written to
     * @param evalFactories
     * @param projectionList
     *            an array of columns to be projected
     */

    public AssignRuntimeFactory(int[] outColumns, IScalarEvaluatorFactory[] evalFactories, int[] projectionList) {
        this(outColumns, evalFactories, projectionList, false);
    }

    public AssignRuntimeFactory(int[] outColumns, IScalarEvaluatorFactory[] evalFactories, int[] projectionList,
            boolean flushFramesRapidly) {
        super(projectionList);
        this.outColumns = outColumns;
        this.evalFactories = evalFactories;
        this.flushFramesRapidly = flushFramesRapidly;
    }

    public int[] getOutColumns() {
        return outColumns;
    }

    public boolean isFlushFramesRapidly() {
        return flushFramesRapidly;
    }

    public IScalarEvaluatorFactory[] getEvalFactories() {
        return evalFactories;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("assign [");
        for (int i = 0; i < outColumns.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(outColumns[i]);
        }
        sb.append("] := [");
        for (int i = 0; i < evalFactories.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(evalFactories[i].toString());
        }
        sb.append("] ");
        sb.append(" project: ");
        sb.append(Arrays.toString(projectionList));
        return sb.toString();
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws HyracksDataException {
        return new AssignRuntime(ctx);
    }

    public int[] getProjectionList() {
        return projectionList;
    }

    public class AssignRuntime extends AbstractOneInputOneOutputOneFramePushRuntime {
        private IPointable result;
        private IScalarEvaluator[] eval;
        protected ArrayTupleBuilder tupleBuilder;
        private final int[] projectionToOutColumns;
        private boolean first = true;
        protected int tupleIndex = 0;
        protected final IHyracksTaskContext ctx;
        protected final IEvaluatorContext evalCtx;

        public AssignRuntime(IHyracksTaskContext ctx) {
            this.ctx = ctx;
            this.evalCtx = new EvaluatorContext(ctx);
            projectionToOutColumns = new int[projectionList.length];
            for (int j = 0; j < projectionList.length; j++) {
                projectionToOutColumns[j] = Arrays.binarySearch(outColumns, projectionList[j]);
            }
            tupleBuilder = new ArrayTupleBuilder(projectionList.length);
            eval = new IScalarEvaluator[evalFactories.length];
            result = VoidPointable.FACTORY.createPointable();
        }

        @Override
        public void open() throws HyracksDataException {
            if (first) {
                initAccessAppendRef(ctx);
                first = false;
                int n = evalFactories.length;
                for (int i = 0; i < n; i++) {
                    eval[i] = evalFactories[i].createScalarEvaluator(evalCtx);
                }
            }
            super.open();
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            // what if nTuple is 0?
            tAccess.reset(buffer);
            int nTuple = tAccess.getTupleCount();
            if (nTuple < 1) {
                if (nTuple < 0) {
                    throw new HyracksDataException("Negative number of tuples in the frame: " + nTuple);
                }
                appender.flush(writer);
            } else {
                if (nTuple > 1) {
                    for (; tupleIndex < nTuple - 1; tupleIndex++) {
                        tRef.reset(tAccess, tupleIndex);
                        produceTuple(tupleBuilder, tAccess, tupleIndex, tRef);
                        appendToFrameFromTupleBuilder(tupleBuilder);
                    }
                }
                if (tupleIndex < nTuple) {
                    tRef.reset(tAccess, tupleIndex);
                    produceTuple(tupleBuilder, tAccess, tupleIndex, tRef);
                    if (flushFramesRapidly) {
                        // Whenever all the tuples in the incoming frame have been consumed, the assign operator
                        // will push its frame to the next operator; i.e., it won't wait until the frame gets full.
                        appendToFrameFromTupleBuilder(tupleBuilder, true);
                    } else {
                        appendToFrameFromTupleBuilder(tupleBuilder);
                    }
                } else {
                    if (flushFramesRapidly) {
                        flushAndReset();
                    }
                }
            }
            tupleIndex = 0;
        }

        protected void produceTuple(ArrayTupleBuilder tb, IFrameTupleAccessor accessor, int tIndex,
                FrameTupleReference tupleRef) throws HyracksDataException {
            try {
                tb.reset();
                for (int f = 0; f < projectionList.length; f++) {
                    int k = projectionToOutColumns[f];
                    if (k >= 0) {
                        eval[k].evaluate(tupleRef, result);
                        tb.addField(result.getByteArray(), result.getStartOffset(), result.getLength());
                    } else {
                        tb.addField(accessor, tIndex, projectionList[f]);
                    }
                }
            } catch (HyracksDataException e) {
                throw HyracksDataException.create(ErrorCode.ERROR_PROCESSING_TUPLE, e, sourceLoc, tupleIndex);
            }
        }

        @Override
        public void flush() throws HyracksDataException {
            appender.flush(writer);
        }
    }
}
