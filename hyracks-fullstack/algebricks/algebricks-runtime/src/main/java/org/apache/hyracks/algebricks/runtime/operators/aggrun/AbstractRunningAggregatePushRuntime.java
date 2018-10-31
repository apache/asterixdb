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

import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
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
    protected final IHyracksTaskContext ctx;
    private final IRunningAggregateEvaluatorFactory[] aggFactories;
    private final Class<T> aggEvalClass;
    protected final List<T> aggEvals;
    private final int[] projectionList;
    private final int[] projectionToOutColumns;
    private final IPointable p = VoidPointable.FACTORY.createPointable();
    private final ArrayTupleBuilder tupleBuilder;
    private boolean first;

    public AbstractRunningAggregatePushRuntime(int[] outColumns, IRunningAggregateEvaluatorFactory[] aggFactories,
            int[] projectionList, IHyracksTaskContext ctx, Class<T> aggEvalClass) {
        this.ctx = ctx;
        this.projectionList = projectionList;
        this.aggFactories = aggFactories;
        this.aggEvalClass = aggEvalClass;
        aggEvals = new ArrayList<>(aggFactories.length);
        tupleBuilder = new ArrayTupleBuilder(projectionList.length);
        projectionToOutColumns = new int[projectionList.length];

        for (int j = 0; j < projectionList.length; j++) {
            projectionToOutColumns[j] = Arrays.binarySearch(outColumns, projectionList[j]);
        }
        first = true;
    }

    @Override
    public void open() throws HyracksDataException {
        super.open();
        if (first) {
            first = false;
            init();
        }
        for (T aggEval : aggEvals) {
            aggEval.init();
        }
    }

    protected void init() throws HyracksDataException {
        initAccessAppendRef(ctx);
        for (IRunningAggregateEvaluatorFactory aggFactory : aggFactories) {
            IRunningAggregateEvaluator aggEval = aggFactory.createRunningAggregateEvaluator(ctx);
            aggEvals.add(aggEvalClass.cast(aggEval));
        }
    }

    protected void produceTuples(IFrameTupleAccessor accessor, int beginIdx, int endIdx) throws HyracksDataException {
        for (int t = beginIdx; t <= endIdx; t++) {
            tRef.reset(accessor, t);
            produceTuple(tupleBuilder, accessor, t, tRef);
            appendToFrameFromTupleBuilder(tupleBuilder);
        }
    }

    private void produceTuple(ArrayTupleBuilder tb, IFrameTupleAccessor accessor, int tIndex,
            FrameTupleReference tupleRef) throws HyracksDataException {
        tb.reset();
        for (int f = 0; f < projectionList.length; f++) {
            int k = projectionToOutColumns[f];
            if (k >= 0) {
                aggEvals.get(k).step(tupleRef, p);
                tb.addField(p.getByteArray(), p.getStartOffset(), p.getLength());
            } else {
                tb.addField(accessor, tIndex, projectionList[f]);
            }
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        appender.flush(writer);
    }
}