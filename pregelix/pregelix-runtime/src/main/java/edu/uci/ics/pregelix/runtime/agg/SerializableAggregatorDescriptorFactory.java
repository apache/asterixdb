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
package edu.uci.ics.pregelix.runtime.agg;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.std.base.ISerializableAggregateFunction;
import edu.uci.ics.pregelix.dataflow.std.base.ISerializableAggregateFunctionFactory;

public class SerializableAggregatorDescriptorFactory implements IAggregatorDescriptorFactory {
    private static final long serialVersionUID = 1L;
    private ISerializableAggregateFunctionFactory aggFuncFactory;

    public SerializableAggregatorDescriptorFactory(ISerializableAggregateFunctionFactory aggFuncFactory) {
        this.aggFuncFactory = aggFuncFactory;
    }

    @Override
    public IAggregatorDescriptor createAggregator(final IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields, int[] keyFieldsInPartialResults, IFrameWriter writer)
            throws HyracksDataException {
        try {
            final FrameTupleReference tupleRef = new FrameTupleReference();
            final FrameTupleReference stateRef = new FrameTupleReference();
            final ISerializableAggregateFunction aggFunc = aggFuncFactory.createAggregateFunction(ctx, writer);

            /**
             * The serializable version aggregator itself is stateless
             */
            return new IAggregatorDescriptor() {

                @Override
                public AggregateState createAggregateStates() {
                    return new AggregateState();
                }

                @Override
                public void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                        AggregateState state) throws HyracksDataException {
                    tupleRef.reset(accessor, tIndex);
                    aggFunc.init(tupleRef, tupleBuilder);
                }

                @Override
                public void reset() {

                }

                @Override
                public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                        int stateTupleIndex, AggregateState state) throws HyracksDataException {
                    tupleRef.reset(accessor, tIndex);
                    stateRef.reset(stateAccessor, stateTupleIndex);
                    aggFunc.step(tupleRef, stateRef);
                }

                @Override
                public boolean outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor,
                        int tIndex, AggregateState state) throws HyracksDataException {
                    stateRef.reset(accessor, tIndex);
                    aggFunc.finishPartial(stateRef, tupleBuilder);
                    return true;
                }

                @Override
                public boolean outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor,
                        int tIndex, AggregateState state) throws HyracksDataException {
                    stateRef.reset(accessor, tIndex);
                    aggFunc.finishFinal(stateRef, tupleBuilder);
                    return true;
                }

                @Override
                public void close() {

                }

            };
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }
}
