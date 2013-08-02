package edu.uci.ics.asterix.metadata.feeds;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.metadata.entities.FeedPolicy;
import edu.uci.ics.asterix.metadata.feeds.FeedRuntime.FeedRuntimeId;
import edu.uci.ics.asterix.metadata.feeds.FeedRuntime.FeedRuntimeState;
import edu.uci.ics.asterix.metadata.feeds.FeedRuntime.FeedRuntimeType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;

public class FeedMetaOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(FeedMetaOperatorDescriptor.class.getName());

    private IOperatorDescriptor coreOperator;
    private final FeedConnectionId feedConnectionId;
    private final FeedPolicy feedPolicy;
    private final FeedRuntimeType runtimeType;

    public FeedMetaOperatorDescriptor(JobSpecification spec, FeedConnectionId feedConnectionId,
            IOperatorDescriptor coreOperatorDescriptor, FeedPolicy feedPolicy, FeedRuntimeType runtimeType) {
        super(spec, coreOperatorDescriptor.getInputArity(), coreOperatorDescriptor.getOutputArity());
        this.feedConnectionId = feedConnectionId;
        this.feedPolicy = feedPolicy;
        if (coreOperatorDescriptor.getOutputRecordDescriptors().length == 1) {
            recordDescriptors[0] = coreOperatorDescriptor.getOutputRecordDescriptors()[0];
        }
        this.coreOperator = coreOperatorDescriptor;
        this.runtimeType = runtimeType;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new FeedMetaNodePushable(ctx, recordDescProvider, partition, nPartitions, coreOperator,
                feedConnectionId, feedPolicy, runtimeType);
    }

    @Override
    public String toString() {
        return "FeedMeta [" + coreOperator + " ]";
    }

    private static class FeedMetaNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

        private AbstractUnaryInputUnaryOutputOperatorNodePushable coreOperatorNodePushable;
        private FeedPolicyEnforcer policyEnforcer;
        private FeedRuntime feedRuntime;
        private FeedConnectionId feedId;
        private int partition;
        private ByteBuffer currentBuffer;
        private final FeedRuntimeType runtimeType;
        private boolean resumeOldState;
        private ExecutorService feedExecService;
        private String nodeId;
        private FrameTupleAccessor fta;

        public FeedMetaNodePushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
                int partition, int nPartitions, IOperatorDescriptor coreOperator, FeedConnectionId feedConnectionId,
                FeedPolicy feedPolicy, FeedRuntimeType runtimeType) throws HyracksDataException {
            this.coreOperatorNodePushable = (AbstractUnaryInputUnaryOutputOperatorNodePushable) ((IActivity) coreOperator)
                    .createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
            this.policyEnforcer = new FeedPolicyEnforcer(feedConnectionId, feedPolicy.getProperties());
            this.partition = partition;
            this.runtimeType = runtimeType;
            this.feedId = feedConnectionId;
            this.nodeId = ctx.getJobletContext().getApplicationContext().getNodeId();
            fta = new FrameTupleAccessor(ctx.getFrameSize(), recordDesc);
        }

        @Override
        public void open() throws HyracksDataException {
            FeedRuntimeId runtimeId = new FeedRuntimeId(runtimeType, feedId, partition);
            try {
                feedRuntime = FeedManager.INSTANCE.getFeedRuntime(runtimeId);
                if (feedRuntime == null) {
                    feedRuntime = new FeedRuntime(feedId, partition, runtimeType);
                    FeedManager.INSTANCE.registerFeedRuntime(feedRuntime);
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Did not find a saved state, starting fresh for " + runtimeType + " node.");
                    }
                    resumeOldState = false;
                } else {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Resuming from saved state (if any) of " + runtimeType + " node.");
                    }
                    resumeOldState = true;
                }
                feedExecService = FeedManager.INSTANCE.getFeedExecutorService(feedId);
                FeedFrameWriter mWriter = new FeedFrameWriter(writer, this, feedId, policyEnforcer, nodeId,
                        runtimeType, partition, fta);
                coreOperatorNodePushable.setOutputFrameWriter(0, mWriter, recordDesc);
                coreOperatorNodePushable.open();
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            try {
                if (resumeOldState) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Old state " + feedRuntime.getRuntimeState().getFrame());
                    }
                    coreOperatorNodePushable.nextFrame(feedRuntime.getRuntimeState().getFrame());
                    feedRuntime.setRuntimeState(null);
                    resumeOldState = false;
                }
                coreOperatorNodePushable.nextFrame(buffer);
            } catch (HyracksDataException e) {
                if (policyEnforcer.getFeedPolicyAccessor().continueOnApplicationFailure()) {
                    boolean isExceptionHarmful = handleException(e.getCause());
                    if (isExceptionHarmful) {
                        // log the tuple
                        FeedRuntimeState runtimeState = new FeedRuntimeState(buffer, writer, e);
                        feedRuntime.setRuntimeState(runtimeState);
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Harmful exception (parked data) " + e);
                        }
                    } else {
                        // ignore the frame (exception is expected)
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Ignoring exception " + e);
                        }
                    }

                } else {
                    throw e;
                }
            }
        }

        private boolean handleException(Throwable exception) {
            if (exception instanceof BTreeDuplicateKeyException) {
                if (resumeOldState) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Received duplicate key exception but that is possible post recovery");
                    }
                    return false;
                } else {
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.warning("Received duplicate key exception!");
                    }
                    return true;
                }
            }
            return true;
        }

        @Override
        public void fail() throws HyracksDataException {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.info("Core Op:" + coreOperatorNodePushable.getDisplayName() + " fail ");
            }
            if (policyEnforcer.getFeedPolicyAccessor().continueOnHardwareFailure()) {
                if (currentBuffer != null) {
                    FeedRuntimeState runtimeState = new FeedRuntimeState(currentBuffer, writer, null);
                    feedRuntime.setRuntimeState(runtimeState);
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Saved feed compute runtime for revivals" + feedRuntime.getFeedRuntimeId());
                    }
                } else {
                    FeedManager.INSTANCE.deRegisterFeedRuntime(feedRuntime.getFeedRuntimeId());
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning(" No state to save, de-registered feed compute runtime "
                                + feedRuntime.getFeedRuntimeId());
                    }
                }
            }
            coreOperatorNodePushable.fail();
        }

        @Override
        public void close() throws HyracksDataException {
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
