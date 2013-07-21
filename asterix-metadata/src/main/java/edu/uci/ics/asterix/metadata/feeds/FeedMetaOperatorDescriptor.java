package edu.uci.ics.asterix.metadata.feeds;

import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.metadata.entities.FeedPolicy;
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

    private static final Logger LOGGER = Logger.getLogger(FeedMetaOperatorDescriptor.class.getName());

    private IOperatorDescriptor coreOperator;
    private final FeedConnectionId feedConnectionId;
    private final FeedPolicy feedPolicy;

    public FeedMetaOperatorDescriptor(JobSpecification spec, FeedConnectionId feedConnectionId,
            IOperatorDescriptor coreOperatorDescriptor, FeedPolicy feedPolicy) {
        super(spec, coreOperatorDescriptor.getInputArity(), coreOperatorDescriptor.getOutputArity());
        this.feedConnectionId = feedConnectionId;
        this.feedPolicy = feedPolicy;
        if (coreOperatorDescriptor.getOutputRecordDescriptors().length == 1) {
            recordDescriptors[0] = coreOperatorDescriptor.getOutputRecordDescriptors()[0];
        }
        this.coreOperator = coreOperatorDescriptor;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new FeedMetaNodePushable(ctx, recordDescProvider, partition, nPartitions, coreOperator,
                feedConnectionId, feedPolicy);
    }

    @Override
    public String toString() {
        return "FeedMeta [" + coreOperator + " ]";
    }

    private static class FeedMetaNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

        private AbstractUnaryInputUnaryOutputOperatorNodePushable coreOperatorNodePushable;
        private FeedPolicyEnforcer policyEnforcer;

        public FeedMetaNodePushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
                int partition, int nPartitions, IOperatorDescriptor coreOperator, FeedConnectionId feedConnectionId,
                FeedPolicy feedPolicy) throws HyracksDataException {
            this.coreOperatorNodePushable = (AbstractUnaryInputUnaryOutputOperatorNodePushable) ((IActivity) coreOperator)
                    .createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
            this.policyEnforcer = new FeedPolicyEnforcer(feedConnectionId, feedPolicy.getProperties());
        }

        @Override
        public void open() throws HyracksDataException {
            coreOperatorNodePushable.setOutputFrameWriter(0, writer, recordDesc);
            coreOperatorNodePushable.open();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Core Op:" + coreOperatorNodePushable.getDisplayName() + " open ");
            }

        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Core Op:" + coreOperatorNodePushable.getDisplayName() + " received frame ");
            }
            try {
                coreOperatorNodePushable.nextFrame(buffer);
            } catch (HyracksDataException e) {
                // log tuple
                if (policyEnforcer.getFeedPolicyAccessor().continueOnHardwareFailure()) {
                } else {
                    throw e;
                }
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Core Op:" + coreOperatorNodePushable.getDisplayName() + " fail ");
            }
            coreOperatorNodePushable.fail();
        }

        @Override
        public void close() throws HyracksDataException {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Core Op:" + coreOperatorNodePushable.getDisplayName() + " close ");
            }
            coreOperatorNodePushable.close();
        }

    }

    public IOperatorDescriptor getCoreOperator() {
        return coreOperator;
    }

    public FeedConnectionId getFeedConnectionId() {
        return feedConnectionId;
    }

    public FeedPolicy getFeedPolicy() {
        return feedPolicy;
    }

}
