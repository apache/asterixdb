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
import edu.uci.ics.hyracks.algebricks.runtime.base.IRunningAggregateEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public class RunningAggregateRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private int[] outColumns;
    private IRunningAggregateEvaluatorFactory[] runningAggregates;

    /**
     * @param outColumns
     *            a sorted array of columns into which the result is written to
     * @param runningAggregates
     * @param projectionList
     *            an array of columns to be projected
     */

    public RunningAggregateRuntimeFactory(int[] outColumns, IRunningAggregateEvaluatorFactory[] runningAggregates,
            int[] projectionList) {
        super(projectionList);
        this.outColumns = outColumns;
        this.runningAggregates = runningAggregates;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("running-aggregate [");
        for (int i = 0; i < outColumns.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(outColumns[i]);
        }
        sb.append("] := [");
        for (int i = 0; i < runningAggregates.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(runningAggregates[i]);
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
            private IPointable p = VoidPointable.FACTORY.createPointable();
            private IRunningAggregateEvaluator[] raggs = new IRunningAggregateEvaluator[runningAggregates.length];
            private ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(projectionList.length);
            private boolean first = true;

            @Override
            public void open() throws HyracksDataException {
                if(!first){
                    FrameUtils.flushFrame(frame, writer);
                    appender.reset(frame, true);
                }
                initAccessAppendRef(ctx);
                if (first) {
                    first = false;
                    int n = runningAggregates.length;
                    for (int i = 0; i < n; i++) {
                        try {
                            raggs[i] = runningAggregates[i].createRunningAggregateEvaluator();
                        } catch (AlgebricksException ae) {
                            throw new HyracksDataException(ae);
                        }
                    }
                }
                for (int i = 0; i < runningAggregates.length; i++) {
                    try {
                        raggs[i].init();
                    } catch (AlgebricksException ae) {
                        throw new HyracksDataException(ae);
                    }
                }
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                for (int t = 0; t < nTuple; t++) {
                    tRef.reset(tAccess, t);
                    produceTuple(tupleBuilder, tAccess, t, tRef);
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
                            raggs[k].step(tupleRef, p);
                        } catch (AlgebricksException e) {
                            throw new HyracksDataException(e);
                        }
                        tb.addField(p.getByteArray(), p.getStartOffset(), p.getLength());
                    } else {
                        tb.addField(accessor, tIndex, projectionList[f]);
                    }
                }
            }

        };
    }
}
