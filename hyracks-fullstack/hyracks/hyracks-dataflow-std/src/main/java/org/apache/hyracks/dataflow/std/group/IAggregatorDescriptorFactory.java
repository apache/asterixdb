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
package org.apache.hyracks.dataflow.std.group;

import java.io.Serializable;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public interface IAggregatorDescriptorFactory extends Serializable {
    IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields, final int[] keyFieldsInPartialResults,
            IFrameWriter writer, long memoryBudget) throws HyracksDataException;

    default IProfiledAggregatorDescriptor createProfiledAggregator(IHyracksTaskContext ctx,
            RecordDescriptor inRecordDescriptor, RecordDescriptor outRecordDescriptor, int[] keyFields,
            final int[] keyFieldsInPartialResults, IFrameWriter writer, long memoryBudget, IOperatorStats stats)
            throws HyracksDataException {
        return new NoOpProfiledAggregator(createAggregator(ctx, inRecordDescriptor, outRecordDescriptor, keyFields,
                keyFieldsInPartialResults, writer, memoryBudget));
    }

    final class NoOpProfiledAggregator implements IProfiledAggregatorDescriptor {
        private final IAggregatorDescriptor delegate;

        public NoOpProfiledAggregator(IAggregatorDescriptor desc) {
            delegate = desc;
        }

        @Override
        public void computeTimings() {
            //no-op
        }

        @Override
        public AggregateState createAggregateStates() throws HyracksDataException {
            return delegate.createAggregateStates();
        }

        @Override
        public void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex, AggregateState state)
                throws HyracksDataException {
            delegate.init(tupleBuilder, accessor, tIndex, state);
        }

        @Override
        public void reset() {
            delegate.reset();
        }

        @Override
        public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                int stateTupleIndex, AggregateState state) throws HyracksDataException {
            delegate.aggregate(accessor, tIndex, stateAccessor, stateTupleIndex, state);

        }

        @Override
        public boolean outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor stateAccessor,
                int tIndex, AggregateState state) throws HyracksDataException {
            return delegate.outputPartialResult(tupleBuilder, stateAccessor, tIndex, state);
        }

        @Override
        public boolean outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor stateAccessor, int tIndex,
                AggregateState state) throws HyracksDataException {
            return delegate.outputFinalResult(tupleBuilder, stateAccessor, tIndex, state);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

}
