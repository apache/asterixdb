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

package org.apache.hyracks.algebricks.runtime.operators.aggreg;

import java.nio.ByteBuffer;

import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;

/**
 * Aggregate operator runtime
 */
public class AggregatePushRuntime extends AbstractOneInputOneOutputOneFramePushRuntime {

    private final IAggregateEvaluatorFactory[] aggFactories;

    private final IEvaluatorContext ctx;

    private IAggregateEvaluator[] aggEvals;

    private IPointable result;

    private ArrayTupleBuilder tupleBuilder;

    private boolean first;

    AggregatePushRuntime(IAggregateEvaluatorFactory[] aggFactories, IHyracksTaskContext ctx) {
        this.aggFactories = aggFactories;
        this.ctx = new EvaluatorContext(ctx);
        aggEvals = new IAggregateEvaluator[aggFactories.length];
        result = VoidPointable.FACTORY.createPointable();
        tupleBuilder = new ArrayTupleBuilder(aggEvals.length);
        first = true;
    }

    @Override
    public void open() throws HyracksDataException {
        if (first) {
            first = false;
            initAccessAppendRef(ctx.getTaskContext());
            for (int i = 0; i < aggFactories.length; i++) {
                aggEvals[i] = aggFactories[i].createAggregateEvaluator(ctx);
            }
        }
        for (int i = 0; i < aggFactories.length; i++) {
            aggEvals[i].init();
        }
        super.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        tAccess.reset(buffer);
        int nTuple = tAccess.getTupleCount();
        for (int t = 0; t < nTuple; t++) {
            tRef.reset(tAccess, t);
            processTuple(tRef);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (isOpen) {
            try {
                finishAggregates(false);
            } finally {
                super.close();
            }
        }
    }

    public void finishAggregates(boolean flushFrame) throws HyracksDataException {
        tupleBuilder.reset();
        for (IAggregateEvaluator aggEval : aggEvals) {
            aggEval.finish(result);
            tupleBuilder.addField(result.getByteArray(), result.getStartOffset(), result.getLength());
        }
        appendToFrameFromTupleBuilder(tupleBuilder, flushFrame);
    }

    private void processTuple(FrameTupleReference tupleRef) throws HyracksDataException {
        for (IAggregateEvaluator aggEval : aggEvals) {
            aggEval.step(tupleRef);
        }
    }
}
