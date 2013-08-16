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
package edu.uci.ics.hyracks.algebricks.runtime.operators.std;

import java.nio.ByteBuffer;
import java.util.Arrays;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public class AssignRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private int[] outColumns;
    private IScalarEvaluatorFactory[] evalFactories;
    private final boolean flushFramesRapidly;

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
            sb.append(evalFactories[i]);
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws AlgebricksException {
        final int[] projectionToOutColumns = new int[projectionList.length];
        for (int j = 0; j < projectionList.length; j++) {
            projectionToOutColumns[j] = Arrays.binarySearch(outColumns, projectionList[j]);
        }

        return new AbstractOneInputOneOutputOneFramePushRuntime() {
            private IPointable result = VoidPointable.FACTORY.createPointable();
            private IScalarEvaluator[] eval = new IScalarEvaluator[evalFactories.length];
            private ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(projectionList.length);
            private boolean first = true;

            @Override
            public void open() throws HyracksDataException {
                if (first) {
                    initAccessAppendRef(ctx);
                    first = false;
                    int n = evalFactories.length;
                    for (int i = 0; i < n; i++) {
                        try {
                            eval[i] = evalFactories[i].createScalarEvaluator(ctx);
                        } catch (AlgebricksException ae) {
                            throw new HyracksDataException(ae);
                        }
                    }
                }
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                int t = 0;
                if (nTuple > 1) {
                    for (; t < nTuple - 1; t++) {
                        tRef.reset(tAccess, t);
                        produceTuple(tupleBuilder, tAccess, t, tRef);
                        appendToFrameFromTupleBuilder(tupleBuilder);
                    }
                }

                tRef.reset(tAccess, t);
                produceTuple(tupleBuilder, tAccess, t, tRef);
                if (flushFramesRapidly) {
                    // Whenever all the tuples in the incoming frame have been consumed, the assign operator 
                    // will push its frame to the next operator; i.e., it won't wait until the frame gets full. 
                    appendToFrameFromTupleBuilder(tupleBuilder, true);
                } else {
                    appendToFrameFromTupleBuilder(tupleBuilder);
                }
            }

            private void produceTuple(ArrayTupleBuilder tb, IFrameTupleAccessor accessor, int tIndex,
                    FrameTupleReference tupleRef) throws HyracksDataException {
                tb.reset();
                for (int f = 0; f < projectionList.length; f++) {
                    int k = projectionToOutColumns[f];
                    if (k >= 0) {
                        try {
                            eval[k].evaluate(tupleRef, result);
                        } catch (AlgebricksException e) {
                            throw new HyracksDataException(e);
                        }
                        tb.addField(result.getByteArray(), result.getStartOffset(), result.getLength());
                    } else {
                        tb.addField(accessor, tIndex, projectionList[f]);
                    }
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }
        };
    }
}
