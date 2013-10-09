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
package edu.uci.ics.pregelix.runtime.simpleagg;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.pregelix.dataflow.group.IClusteredAggregatorDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IAggregateFunction;
import edu.uci.ics.pregelix.dataflow.std.base.IAggregateFunctionFactory;

public class AccumulatingAggregatorFactory implements IClusteredAggregatorDescriptorFactory {

    private static final long serialVersionUID = 1L;
    private IAggregateFunctionFactory[] aggFactories;

    public AccumulatingAggregatorFactory(IAggregateFunctionFactory[] aggFactories) {
        this.aggFactories = aggFactories;
    }

    @SuppressWarnings("unchecked")
    @Override
    public IAggregatorDescriptor createAggregator(final IHyracksTaskContext ctx, RecordDescriptor inRecordDesc,
            RecordDescriptor outRecordDescriptor, final int[] groupFields, int[] partialgroupFields,
            final IFrameWriter writer, final ByteBuffer outputFrame, final FrameTupleAppender appender)
            throws HyracksDataException {
        final int frameSize = ctx.getFrameSize();
        final ArrayTupleBuilder internalTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);

        return new IAggregatorDescriptor() {
            private FrameTupleReference ftr = new FrameTupleReference();
            private int groupKeySize = 0;
            private int metaSlotSize = 4;

            @Override
            public AggregateState createAggregateStates() {
                IAggregateFunction[] agg = new IAggregateFunction[aggFactories.length];
                ArrayBackedValueStorage[] aggOutput = new ArrayBackedValueStorage[aggFactories.length];
                for (int i = 0; i < agg.length; i++) {
                    aggOutput[i] = new ArrayBackedValueStorage();
                    try {
                        agg[i] = aggFactories[i].createAggregateFunction(ctx, aggOutput[i], writer);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }
                return new AggregateState(Pair.of(aggOutput, agg));
            }

            @Override
            public void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                setGroupKeySize(accessor, tIndex);
                initAggregateFunctions(state, true);
                int stateSize = estimateStep(accessor, tIndex, state);
                if (stateSize > frameSize) {
                    throw new HyracksDataException(
                            "Message combiner intermediate data size "
                                    + stateSize
                                    + " is larger than frame size! Check the size estimattion implementation in the message combiner.");
                }
                singleStep(accessor, tIndex, state);
            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                    int stateTupleIndex, AggregateState state) throws HyracksDataException {
                int stateSize = estimateStep(accessor, tIndex, state);
                if (stateSize > frameSize) {
                    emitResultTuple(accessor, tIndex, state);
                    initAggregateFunctions(state, false);
                }
                singleStep(accessor, tIndex, state);
            }

            @Override
            public boolean outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                Pair<ArrayBackedValueStorage[], IAggregateFunction[]> aggState = (Pair<ArrayBackedValueStorage[], IAggregateFunction[]>) state.state;
                ArrayBackedValueStorage[] aggOutput = aggState.getLeft();
                IAggregateFunction[] agg = aggState.getRight();
                for (int i = 0; i < agg.length; i++) {
                    try {
                        agg[i].finishAll();
                        tupleBuilder.addField(aggOutput[i].getByteArray(), aggOutput[i].getStartOffset(),
                                aggOutput[i].getLength());
                    } catch (Exception e) {
                        throw new HyracksDataException(e);
                    }
                }
                return true;
            }

            @Override
            public void reset() {

            }

            @Override
            public boolean outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                throw new IllegalStateException("this method should not be called");
            }

            @Override
            public void close() {

            }

            private void initAggregateFunctions(AggregateState state, boolean all) throws HyracksDataException {
                Pair<ArrayBackedValueStorage[], IAggregateFunction[]> aggState = (Pair<ArrayBackedValueStorage[], IAggregateFunction[]>) state.state;
                ArrayBackedValueStorage[] aggOutput = aggState.getLeft();
                IAggregateFunction[] agg = aggState.getRight();

                /**
                 * initialize aggregate functions
                 */
                for (int i = 0; i < agg.length; i++) {
                    aggOutput[i].reset();
                    try {
                        if (all) {
                            agg[i].initAll();
                        } else {
                            agg[i].init();
                        }
                    } catch (Exception e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            private void singleStep(IFrameTupleAccessor accessor, int tIndex, AggregateState state)
                    throws HyracksDataException {
                Pair<ArrayBackedValueStorage[], IAggregateFunction[]> aggState = (Pair<ArrayBackedValueStorage[], IAggregateFunction[]>) state.state;
                IAggregateFunction[] agg = aggState.getRight();
                ftr.reset(accessor, tIndex);
                for (int i = 0; i < agg.length; i++) {
                    try {
                        agg[i].step(ftr);
                    } catch (Exception e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            private int estimateStep(IFrameTupleAccessor accessor, int tIndex, AggregateState state)
                    throws HyracksDataException {
                int size = metaSlotSize + groupKeySize;
                Pair<ArrayBackedValueStorage[], IAggregateFunction[]> aggState = (Pair<ArrayBackedValueStorage[], IAggregateFunction[]>) state.state;
                IAggregateFunction[] agg = aggState.getRight();
                ftr.reset(accessor, tIndex);
                for (int i = 0; i < agg.length; i++) {
                    try {
                        size += agg[i].estimateStep(ftr) + metaSlotSize;
                    } catch (Exception e) {
                        throw new HyracksDataException(e);
                    }
                }
                return size * 2;
            }

            private void emitResultTuple(IFrameTupleAccessor accessor, int tIndex, AggregateState state)
                    throws HyracksDataException {
                internalTupleBuilder.reset();
                for (int j = 0; j < groupFields.length; j++) {
                    internalTupleBuilder.addField(accessor, tIndex, groupFields[j]);
                }
                Pair<ArrayBackedValueStorage[], IAggregateFunction[]> aggState = (Pair<ArrayBackedValueStorage[], IAggregateFunction[]>) state.state;
                ArrayBackedValueStorage[] aggOutput = aggState.getLeft();
                IAggregateFunction[] agg = aggState.getRight();
                for (int i = 0; i < agg.length; i++) {
                    try {
                        agg[i].finish();
                        internalTupleBuilder.addField(aggOutput[i].getByteArray(), aggOutput[i].getStartOffset(),
                                aggOutput[i].getLength());
                    } catch (Exception e) {
                        throw new HyracksDataException(e);
                    }
                }
                if (!appender.appendSkipEmptyField(internalTupleBuilder.getFieldEndOffsets(),
                        internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                    FrameUtils.flushFrame(outputFrame, writer);
                    appender.reset(outputFrame, true);
                    if (!appender.appendSkipEmptyField(internalTupleBuilder.getFieldEndOffsets(),
                            internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                        throw new HyracksDataException("The output cannot be fit into a frame.");
                    }
                }
            }

            public void setGroupKeySize(IFrameTupleAccessor accessor, int tIndex) {
                groupKeySize = 0;
                for (int i = 0; i < groupFields.length; i++) {
                    int fIndex = groupFields[i];
                    int fStartOffset = accessor.getFieldStartOffset(tIndex, fIndex);
                    int fLen = accessor.getFieldEndOffset(tIndex, fIndex) - fStartOffset;
                    groupKeySize += fLen + metaSlotSize;
                }
            }

        };
    }
}
