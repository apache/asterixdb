package edu.uci.ics.hyracks.examples.tpch.helper;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.group.IAccumulatingAggregator;
import edu.uci.ics.hyracks.dataflow.std.group.IAccumulatingAggregatorFactory;

public class CountAccumulatingAggregatorFactory implements IAccumulatingAggregatorFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public IAccumulatingAggregator createAggregator(RecordDescriptor inRecordDesc, RecordDescriptor outRecordDescriptor) {
        final ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
        return new IAccumulatingAggregator() {
            private int count;

            @Override
            public boolean output(FrameTupleAppender appender, IFrameTupleAccessor accessor, int tIndex)
                    throws HyracksDataException {
                tb.reset();
                tb.addField(accessor, tIndex, 0);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, count);
                return appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
            }

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                count = 0;
            }

            @Override
            public void accumulate(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                ++count;
            }
        };
    }
}