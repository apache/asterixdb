package edu.uci.ics.asterix.metadata.feeds;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class FeedMetaOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private IOperatorDescriptor coreOperator;

    public FeedMetaOperatorDescriptor(JobSpecification spec, IOperatorDescriptor coreOperatorDescriptor) {
        super(spec, coreOperatorDescriptor.getInputArity(), coreOperatorDescriptor.getOutputArity());
        this.coreOperator = coreOperatorDescriptor;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new FeedMetaNodePushable(ctx, recordDescProvider, partition, nPartitions, coreOperator);
    }

    private static class FeedMetaNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

        private AbstractUnaryInputUnaryOutputOperatorNodePushable coreOperatorNodePushable;

        public FeedMetaNodePushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
                int partition, int nPartitions, IOperatorDescriptor coreOperator) throws HyracksDataException {
            this.coreOperatorNodePushable = (AbstractUnaryInputUnaryOutputOperatorNodePushable) ((IActivity) coreOperator)
                    .createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
        }

        @Override
        public void open() throws HyracksDataException {
            coreOperatorNodePushable.open();
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            coreOperatorNodePushable.nextFrame(buffer);
        }

        @Override
        public void fail() throws HyracksDataException {
            coreOperatorNodePushable.fail();
        }

        @Override
        public void close() throws HyracksDataException {
            coreOperatorNodePushable.close();
        }

    }

}
