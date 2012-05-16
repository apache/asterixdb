package edu.uci.ics.hyracks.dataflow.std.misc;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputOperatorNodePushable;

public class SplitOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    public SplitOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc, int outputArity) {
        super(spec, 1, outputArity);
        for (int i = 0; i < outputArity; i++) {
            recordDescriptors[i] = rDesc;
        }
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
            throws HyracksDataException {
        return new AbstractUnaryInputOperatorNodePushable() {
            private final IFrameWriter[] writers = new IFrameWriter[outputArity];

            @Override
            public void close() throws HyracksDataException {
                for (IFrameWriter writer : writers) {
                    writer.close();
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                for (IFrameWriter writer : writers) {
                    writer.fail();
                }
            }

            @Override
            public void nextFrame(ByteBuffer bufferAccessor) throws HyracksDataException {
                for (IFrameWriter writer : writers) {
                    FrameUtils.flushFrame(bufferAccessor, writer);
                }
            }

            @Override
            public void open() throws HyracksDataException {
                for (IFrameWriter writer : writers) {
                    writer.open();
                }
            }

            @Override
            public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
                writers[index] = writer;
            }
        };
    }
}
