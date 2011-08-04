/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.std.aggregators;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class MultiAggregatorDescriptorFactory implements IAggregatorDescriptorFactory {
    private static final long serialVersionUID = 1L;
    private final IAggregatorDescriptorFactory[] aggregatorFactories;

    public MultiAggregatorDescriptorFactory(IAggregatorDescriptorFactory[] aggregatorFactories) {
        this.aggregatorFactories = aggregatorFactories;
    }

    @Override
    public IAggregatorDescriptor createAggregator(final IHyracksStageletContext ctx,
            final RecordDescriptor inRecordDescriptor, final RecordDescriptor outRecordDescriptor, final int[] keyFields)
            throws HyracksDataException {

        final IAggregatorDescriptor[] aggregators = new IAggregatorDescriptor[this.aggregatorFactories.length];
        for (int i = 0; i < aggregators.length; i++) {
            aggregators[i] = aggregatorFactories[i].createAggregator(ctx, inRecordDescriptor, outRecordDescriptor,
                    keyFields);
        }

        return new IAggregatorDescriptor() {

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tupleBuilder)
                    throws HyracksDataException {
                for (int i = 0; i < aggregators.length; i++) {
                    aggregators[i].init(accessor, tIndex, tupleBuilder);
                }
            }

            @Override
            public int aggregate(IFrameTupleAccessor accessor, int tIndex, byte[] data, int offset, int length)
                    throws HyracksDataException {
                int adjust = 0;
                for (int i = 0; i < aggregators.length; i++) {
                    adjust += aggregators[i].aggregate(accessor, tIndex, data, offset + adjust, length - adjust);
                }
                return adjust;
            }

            @Override
            public void outputPartialResult(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tupleBuilder)
                    throws HyracksDataException {
                for (int i = 0; i < aggregators.length; i++) {
                    aggregators[i].outputPartialResult(accessor, tIndex, tupleBuilder);
                }
            }

            @Override
            public void outputResult(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tupleBuilder)
                    throws HyracksDataException {
                for (int i = 0; i < aggregators.length; i++) {
                    aggregators[i].outputResult(accessor, tIndex, tupleBuilder);
                }
            }

            @Override
            public void close() {
                for (int i = 0; i < aggregators.length; i++) {
                    aggregators[i].close();
                }
            }

            @Override
            public void reset() {
                for (int i = 0; i < aggregators.length; i++) {
                    aggregators[i].reset();
                }
            }

        };
    }
}
