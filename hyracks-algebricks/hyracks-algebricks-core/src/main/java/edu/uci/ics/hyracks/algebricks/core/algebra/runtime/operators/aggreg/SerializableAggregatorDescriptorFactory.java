package edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.aggreg;

import java.io.DataOutput;

import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.ISerializableAggregateFunction;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.ISerializableAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IAggregatorDescriptorFactory;

public class SerializableAggregatorDescriptorFactory implements IAggregatorDescriptorFactory {
    private static final long serialVersionUID = 1L;

    private ISerializableAggregateFunctionFactory[] aggFactories;

    public SerializableAggregatorDescriptorFactory(ISerializableAggregateFunctionFactory[] aggFactories) {
        this.aggFactories = aggFactories;
    }

    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields) throws HyracksDataException {
        final int[] keys = keyFields;
        final int OFFSET_INT_LENGTH = 4;

        /**
         * one IAggregatorDescriptor instance per Gby operator
         */
        return new IAggregatorDescriptor() {
            private FrameTupleReference ftr = new FrameTupleReference();
            private ISerializableAggregateFunction[] aggs = new ISerializableAggregateFunction[aggFactories.length];
            private int offsetFieldIndex = keys.length;
            private int stateFieldLength[] = new int[aggFactories.length];

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tb)
                    throws HyracksDataException {
                DataOutput output = tb.getDataOutput();
                ftr.reset(accessor, tIndex);
                int startSize = tb.getSize();
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        int begin = tb.getSize();
                        if (aggs[i] == null) {
                            aggs[i] = aggFactories[i].createAggregateFunction();
                        }
                        aggs[i].init(output);
                        tb.addFieldEndOffset();
                        stateFieldLength[i] = tb.getSize() - begin;
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
                int startOffset = 0;
                if (offsetFieldIndex > 0)
                    startOffset = tb.getFieldEndOffsets()[offsetFieldIndex - 1];
                else
                    startOffset = 0;
                int endSize = tb.getSize();
                int len = endSize - startSize;
                aggregate(accessor, tIndex, tb.getByteArray(), startOffset, len);
            }

            @Override
            public int aggregate(IFrameTupleAccessor accessor, int tIndex, byte[] data, int offset, int length)
                    throws HyracksDataException {
                ftr.reset(accessor, tIndex);
                int start = offset;
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        aggs[i].step(ftr, data, start, stateFieldLength[i]);
                        start += stateFieldLength[i];
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
                return OFFSET_INT_LENGTH;
            }

            @Override
            public void outputPartialResult(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tb)
                    throws HyracksDataException {
                byte[] data = accessor.getBuffer().array();
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int aggFieldOffset = accessor.getFieldStartOffset(tIndex, offsetFieldIndex);
                int refOffset = startOffset + accessor.getFieldSlotsLength() + aggFieldOffset;
                int start = refOffset;
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        aggs[i].finishPartial(data, start, stateFieldLength[i], tb.getDataOutput());
                        start += stateFieldLength[i];
                        tb.addFieldEndOffset();
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public void outputResult(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tb)
                    throws HyracksDataException {
                byte[] data = accessor.getBuffer().array();
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int aggFieldOffset = accessor.getFieldStartOffset(tIndex, offsetFieldIndex);
                int refOffset = startOffset + accessor.getFieldSlotsLength() + aggFieldOffset;
                int start = refOffset;
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        aggs[i].finish(data, start, stateFieldLength[i], tb.getDataOutput());
                        start += stateFieldLength[i];
                        tb.addFieldEndOffset();
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public void reset() {
            }

            @Override
            public void close() {
                reset();
            }

        };
    }
}
