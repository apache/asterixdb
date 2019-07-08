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

package org.apache.hyracks.algebricks.runtime.operators.aggrun;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public abstract class AbstractRunningAggregatePushRuntime<T extends IRunningAggregateEvaluator>
        extends AbstractOneInputOneOutputOneFramePushRuntime {
    protected final IEvaluatorContext ctx;
    private final IRunningAggregateEvaluatorFactory[] runningAggFactories;
    private final Class<T> runningAggEvalClass;
    protected final List<T> runningAggEvals;
    private final int[] projectionColumns;
    private final int[] projectionToOutColumns;
    private final IPointable p = VoidPointable.FACTORY.createPointable();
    protected ArrayTupleBuilder tupleBuilder;
    private boolean isFirst;

    public AbstractRunningAggregatePushRuntime(int[] projectionColumns, int[] runningAggOutColumns,
            IRunningAggregateEvaluatorFactory[] runningAggFactories, Class<T> runningAggEvalClass,
            IHyracksTaskContext ctx) {
        this.ctx = new EvaluatorContext(ctx);
        this.projectionColumns = projectionColumns;
        this.runningAggFactories = runningAggFactories;
        this.runningAggEvalClass = runningAggEvalClass;
        runningAggEvals = new ArrayList<>(runningAggFactories.length);
        projectionToOutColumns = new int[projectionColumns.length];
        for (int j = 0; j < projectionColumns.length; j++) {
            projectionToOutColumns[j] = Arrays.binarySearch(runningAggOutColumns, projectionColumns[j]);
        }
        isFirst = true;
    }

    @Override
    public void open() throws HyracksDataException {
        super.open();
        if (isFirst) {
            isFirst = false;
            init();
        }
        for (T runningAggEval : runningAggEvals) {
            runningAggEval.init();
        }
    }

    protected void init() throws HyracksDataException {
        tupleBuilder = createOutputTupleBuilder(projectionColumns);
        initAccessAppendRef(ctx.getTaskContext());
        for (IRunningAggregateEvaluatorFactory runningAggFactory : runningAggFactories) {
            IRunningAggregateEvaluator runningAggEval = runningAggFactory.createRunningAggregateEvaluator(ctx);
            runningAggEvals.add(runningAggEvalClass.cast(runningAggEval));
        }
    }

    protected ArrayTupleBuilder createOutputTupleBuilder(int[] projectionList) {
        return new ArrayTupleBuilder(projectionList.length);
    }

    protected void produceTuples(IFrameTupleAccessor accessor, int beginIdx, int endIdx, FrameTupleReference tupleRef)
            throws HyracksDataException {
        for (int t = beginIdx; t <= endIdx; t++) {
            tupleRef.reset(accessor, t);
            produceTuple(tupleBuilder, accessor, t, tupleRef);
            appendToFrameFromTupleBuilder(tupleBuilder);
        }
    }

    protected void produceTuple(ArrayTupleBuilder tb, IFrameTupleAccessor accessor, int tIndex,
            FrameTupleReference tupleRef) throws HyracksDataException {
        tb.reset();
        for (int f = 0; f < projectionColumns.length; f++) {
            int k = projectionToOutColumns[f];
            if (k >= 0) {
                runningAggEvals.get(k).step(tupleRef, p);
                tb.addField(p.getByteArray(), p.getStartOffset(), p.getLength());
            } else {
                tb.addField(accessor, tIndex, projectionColumns[f]);
            }
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        appender.flush(writer);
    }
}
