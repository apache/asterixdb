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
package edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.aggreg;

import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IAggregateFunction;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.std.group.IAccumulatingAggregator;
import edu.uci.ics.hyracks.dataflow.std.group.IAccumulatingAggregatorFactory;

public class SimpleAlgebricksAccumulatingAggregatorFactory implements IAccumulatingAggregatorFactory {

    private static final long serialVersionUID = 1L;
    private IAggregateFunctionFactory[] aggFactories;
    private int[] keys;
    private int[] fdColumns;

    public SimpleAlgebricksAccumulatingAggregatorFactory(IAggregateFunctionFactory[] aggFactories, int[] keys,
            int[] fdColumns) {
        this.aggFactories = aggFactories;
        this.keys = keys;
        this.fdColumns = fdColumns;
    }

    @Override
    public IAccumulatingAggregator createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDesc,
            RecordDescriptor outRecordDescriptor) throws HyracksDataException {

        final IAggregateFunction[] agg = new IAggregateFunction[aggFactories.length];
        final ArrayBackedValueStorage[] aggOutput = new ArrayBackedValueStorage[aggFactories.length];
        for (int i = 0; i < agg.length; i++) {
            aggOutput[i] = new ArrayBackedValueStorage();
            try {
                agg[i] = aggFactories[i].createAggregateFunction(aggOutput[i]);
            } catch (AlgebricksException e) {
                throw new HyracksDataException(e);
            }
        }

        return new IAccumulatingAggregator() {

            private FrameTupleReference ftr = new FrameTupleReference();
            private ArrayTupleBuilder tb = new ArrayTupleBuilder(keys.length + fdColumns.length + agg.length);
            private boolean pending;

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                pending = false;
                for (int i = 0; i < aggOutput.length; i++) {
                    aggOutput[i].reset();
                    try {
                        agg[i].init();
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
                tb.reset();
                for (int i = 0; i < keys.length; ++i) {
                    tb.addField(accessor, tIndex, keys[i]);
                }
                for (int i = 0; i < fdColumns.length; i++) {
                    tb.addField(accessor, tIndex, fdColumns[i]);
                }

            }

            @Override
            public void accumulate(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                ftr.reset(accessor, tIndex);
                for (int i = 0; i < agg.length; i++) {
                    try {
                        agg[i].step(ftr);
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public boolean output(FrameTupleAppender appender, IFrameTupleAccessor accessor, int tIndex,
                    int[] keyFieldIndexes) throws HyracksDataException {
                if (!pending) {
                    for (int i = 0; i < agg.length; i++) {
                        try {
                            agg[i].finish();
                            tb.addField(aggOutput[i].getBytes(), aggOutput[i].getStartIndex(), aggOutput[i].getLength());
                        } catch (AlgebricksException e) {
                            throw new HyracksDataException(e);
                        }
                    }
                }
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    pending = true;
                    return false;
                } else {
                    return true;
                }
            }

        };
    }

}
