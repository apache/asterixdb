package edu.uci.ics.hyracks.dataflow.std.misc;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

public class SinkOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    public SinkOperatorDescriptor(IOperatorDescriptorRegistry spec, int nInput) {
        super(spec, nInput, 0);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            @Override
            public void open() throws HyracksDataException {
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            }

            @Override
            public void close() throws HyracksDataException {
            }

            @Override
            public void fail() throws HyracksDataException {
            }
        };
    }
}