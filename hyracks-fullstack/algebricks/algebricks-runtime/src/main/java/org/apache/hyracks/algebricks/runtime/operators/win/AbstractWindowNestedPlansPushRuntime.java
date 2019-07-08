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

package org.apache.hyracks.algebricks.runtime.operators.win;

import org.apache.hyracks.algebricks.data.IBinaryBooleanInspector;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PointableTupleReference;

/**
 * Base class for window runtime implementations that compute nested aggregates
 */
abstract class AbstractWindowNestedPlansPushRuntime extends WindowMaterializingPushRuntime {

    final int nestedAggOutSchemaSize;

    private final WindowAggregatorDescriptorFactory nestedAggFactory;

    private IWindowAggregatorDescriptor nestedAgg;

    AbstractWindowNestedPlansPushRuntime(int[] partitionColumns,
            IBinaryComparatorFactory[] partitionComparatorFactories,
            IBinaryComparatorFactory[] orderComparatorFactories, int[] projectionColumns, int[] runningAggOutColumns,
            IRunningAggregateEvaluatorFactory[] runningAggFactories, int nestedAggOutSchemaSize,
            WindowAggregatorDescriptorFactory nestedAggFactory, IHyracksTaskContext ctx, int memSizeInFrames,
            SourceLocation sourceLoc) {
        super(partitionColumns, partitionComparatorFactories, orderComparatorFactories, projectionColumns,
                runningAggOutColumns, runningAggFactories, ctx, memSizeInFrames, sourceLoc);
        this.nestedAggFactory = nestedAggFactory;
        this.nestedAggOutSchemaSize = nestedAggOutSchemaSize;
    }

    @Override
    protected void init() throws HyracksDataException {
        super.init();
        nestedAgg = nestedAggCreate();
    }

    @Override
    public void close() throws HyracksDataException {
        super.close();
        nestedAgg.close();
    }

    @Override
    protected ArrayTupleBuilder createOutputTupleBuilder(int[] projectionList) {
        return new ArrayTupleBuilder(projectionList.length + nestedAggOutSchemaSize);
    }

    final IWindowAggregatorDescriptor nestedAggCreate() throws HyracksDataException {
        return nestedAggFactory.createAggregator(ctx.getTaskContext(), null, null, null, null, -1);
    }

    /**
     * Aggregator created by
     * {@link WindowAggregatorDescriptorFactory#createAggregator(IHyracksTaskContext, RecordDescriptor,
     * RecordDescriptor, int[], int[], long) WindowAggregatorDescriptorFactory.createAggregator(...)}
     * does not process argument tuple in init()
     */
    final void nestedAggInit() throws HyracksDataException {
        nestedAggInit(nestedAgg);
    }

    static void nestedAggInit(IWindowAggregatorDescriptor nestedAgg) throws HyracksDataException {
        nestedAgg.init(null, null, -1, null);
    }

    final void nestedAggAggregate(FrameTupleAccessor tAccess, int tIndex) throws HyracksDataException {
        nestedAggAggregate(nestedAgg, tAccess, tIndex);
    }

    static void nestedAggAggregate(IWindowAggregatorDescriptor nestedAgg, FrameTupleAccessor tAccess, int tIndex)
            throws HyracksDataException {
        nestedAgg.aggregate(tAccess, tIndex, null, -1, null);
    }

    final void nestedAggOutputFinalResult(ArrayTupleBuilder outTupleBuilder) throws HyracksDataException {
        nestedAggOutputFinalResult(nestedAgg, outTupleBuilder);
    }

    static void nestedAggOutputFinalResult(IWindowAggregatorDescriptor nestedAgg, ArrayTupleBuilder outTupleBuilder)
            throws HyracksDataException {
        nestedAgg.outputFinalResult(outTupleBuilder, null, -1, null);
    }

    final void nestedAggOutputPartialResult(ArrayTupleBuilder outTupleBuilder) throws HyracksDataException {
        nestedAggOutputPartialResult(nestedAgg, outTupleBuilder);
    }

    static boolean nestedAggOutputPartialResult(IWindowAggregatorDescriptor nestedAgg,
            ArrayTupleBuilder outTupleBuilder) throws HyracksDataException {
        return nestedAgg.outputPartialResult(outTupleBuilder, null, -1, null);
    }

    final void nestAggDiscardFinalResult() throws HyracksDataException {
        nestAggDiscardFinalResult(nestedAgg);
    }

    static void nestAggDiscardFinalResult(IWindowAggregatorDescriptor nestedAgg) throws HyracksDataException {
        nestedAgg.discardFinalResult();
    }

    static IScalarEvaluator[] createEvaluators(IScalarEvaluatorFactory[] evalFactories, IEvaluatorContext ctx)
            throws HyracksDataException {
        IScalarEvaluator[] evals = new IScalarEvaluator[evalFactories.length];
        for (int i = 0; i < evalFactories.length; i++) {
            evals[i] = evalFactories[i].createScalarEvaluator(ctx);
        }
        return evals;
    }

    static void evaluate(IScalarEvaluator[] evals, IFrameTupleReference inTuple, PointableTupleReference outTuple)
            throws HyracksDataException {
        for (int i = 0; i < evals.length; i++) {
            evals[i].evaluate(inTuple, outTuple.getField(i));
        }
    }

    static boolean allTrue(ITupleReference tupleRef, IBinaryBooleanInspector boolAccessor) throws HyracksDataException {
        for (int i = 0, n = tupleRef.getFieldCount(); i < n; i++) {
            boolean v = boolAccessor.getBooleanValue(tupleRef.getFieldData(i), tupleRef.getFieldStart(i),
                    tupleRef.getFieldLength(i));
            if (!v) {
                return false;
            }
        }
        return true;
    }
}
