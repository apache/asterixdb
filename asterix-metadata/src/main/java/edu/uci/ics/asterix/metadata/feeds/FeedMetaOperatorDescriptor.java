package edu.uci.ics.asterix.metadata.feeds;

import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedRuntime;
import edu.uci.ics.asterix.common.feeds.FeedRuntime.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.FeedRuntime.FeedRuntimeState;
import edu.uci.ics.asterix.common.feeds.FeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.IFeedManager;
import edu.uci.ics.asterix.metadata.entities.FeedPolicy;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;

/**
 * FeedMetaOperatorDescriptor is a wrapper operator that provides a sanboox like
 * environment for an hyracks operator that is part of a feed ingestion pipeline.
 * The MetaFeed operator provides an interface iden- tical to that offered by the
 * underlying wrapped operator, hereafter referred to as the core operator.
 * As seen by Hyracks, the altered pipeline is identical to the earlier version formed
 * from core operators. The MetaFeed operator enhances each core operator by providing
 * functionality for handling runtime exceptions, saving any state for future retrieval,
 * and measuring/reporting of performance characteristics. We next describe how the added
 * functionality contributes to providing fault- tolerance.
 */

public class FeedMetaOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(FeedMetaOperatorDescriptor.class.getName());

    /** The actual (Hyracks) operator that is wrapped around by the Metafeed Adapter **/
    private IOperatorDescriptor coreOperator;

    /**
     * A unique identifier for the feed instance. A feed instance represents the flow of data
     * from a feed to a dataset.
     **/
    private final FeedConnectionId feedConnectionId;

    /**
     * The policy associated with the feed instance.
     */
    private final FeedPolicy feedPolicy;

    /**
     * type for the feed runtime associated with the operator.
     * Possible values: INGESTION, COMPUTE, STORAGE, COMMIT
     */
    private final FeedRuntimeType runtimeType;

    private final String operandId;

    public FeedMetaOperatorDescriptor(JobSpecification spec, FeedConnectionId feedConnectionId,
            IOperatorDescriptor coreOperatorDescriptor, FeedPolicy feedPolicy, FeedRuntimeType runtimeType,
            String operandId) {
        super(spec, coreOperatorDescriptor.getInputArity(), coreOperatorDescriptor.getOutputArity());
        this.feedConnectionId = feedConnectionId;
        this.feedPolicy = feedPolicy;
        if (coreOperatorDescriptor.getOutputRecordDescriptors().length == 1) {
            recordDescriptors[0] = coreOperatorDescriptor.getOutputRecordDescriptors()[0];
        }
        this.coreOperator = coreOperatorDescriptor;
        this.runtimeType = runtimeType;
        this.operandId = operandId;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new FeedMetaNodePushable(ctx, recordDescProvider, partition, nPartitions, coreOperator,
                feedConnectionId, feedPolicy, runtimeType, operandId);
    }

    @Override
    public String toString() {
        return "FeedMeta [" + coreOperator + " ]";
    }

    private static class FeedMetaNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

        /** Runtime node pushable corresponding to the core feed operator **/
        private AbstractUnaryInputUnaryOutputOperatorNodePushable coreOperatorNodePushable;

        /**
         * A policy enforcer that ensures dyanmic decisions for a feed are taken in accordance
         * with the associated ingestion policy
         **/
        private FeedPolicyEnforcer policyEnforcer;

        /**
         * The Feed Runtime instance associated with the operator. Feed Runtime captures the state of the operator while
         * the feed is active.
         */
        private FeedRuntime feedRuntime;

        /**
         * A unique identifier for the feed instance. A feed instance represents the flow of data
         * from a feed to a dataset.
         **/
        private FeedConnectionId feedId;

        /** Denotes the i'th operator instance in a setting where K operator instances are scheduled to run in parallel **/
        private int partition;

        /** A buffer that is used to hold the current frame that is being processed **/
        private ByteBuffer currentBuffer;

        /** Type associated with the core feed operator **/
        private final FeedRuntimeType runtimeType;

        /** True is the feed is recovering from a previous failed execution **/
        private boolean resumeOldState;

        /** The Node Controller ID for the host NC **/

        private String nodeId;

        /** Allows to iterate over the tuples in a frame **/
        private FrameTupleAccessor fta;

        /** The (singleton) instance of IFeedManager **/
        private IFeedManager feedManager;

        private final String operandId;

        public FeedMetaNodePushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
                int partition, int nPartitions, IOperatorDescriptor coreOperator, FeedConnectionId feedConnectionId,
                FeedPolicy feedPolicy, FeedRuntimeType runtimeType, String operationId) throws HyracksDataException {
            this.coreOperatorNodePushable = (AbstractUnaryInputUnaryOutputOperatorNodePushable) ((IActivity) coreOperator)
                    .createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
            this.policyEnforcer = new FeedPolicyEnforcer(feedConnectionId, feedPolicy.getProperties());
            this.partition = partition;
            this.runtimeType = runtimeType;
            this.feedId = feedConnectionId;
            this.nodeId = ctx.getJobletContext().getApplicationContext().getNodeId();
            fta = new FrameTupleAccessor(ctx.getFrameSize(), recordDesc);
            IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                    .getApplicationContext().getApplicationObject();
            this.feedManager = runtimeCtx.getFeedManager();
            this.operandId = operationId;
        }

        @Override
        public void open() throws HyracksDataException {
            FeedRuntimeId runtimeId = new FeedRuntimeId(feedId, runtimeType, operandId, partition);
            try {
                feedRuntime = feedManager.getFeedRuntime(runtimeId);
                if (feedRuntime == null) {
                    feedRuntime = new FeedRuntime(feedId, partition, runtimeType, operandId);
                    feedManager.registerFeedRuntime(feedRuntime);
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Did not find a saved state from a previous zombie, starting a new instance for "
                                + runtimeType + " node.");
                    }
                    resumeOldState = false;
                } else {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Retreived state from the zombie instance from previous execution for "
                                + runtimeType + " node.");
                    }
                    resumeOldState = true;
                }
                FeedFrameWriter mWriter = new FeedFrameWriter(writer, this, feedId, policyEnforcer, nodeId,
                        runtimeType, partition, fta, feedManager);
                coreOperatorNodePushable.setOutputFrameWriter(0, mWriter, recordDesc);
                coreOperatorNodePushable.open();
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe("Unable to initialize feed operator " + feedRuntime + " [" + partition + "]");
                }
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            try {
                if (resumeOldState) {
                    FeedRuntimeState runtimeState = feedRuntime.getRuntimeState();
                    if (runtimeState != null) {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("State from previous zombie instance " + feedRuntime.getRuntimeState());
                        }
                        coreOperatorNodePushable.nextFrame(feedRuntime.getRuntimeState().getFrame());
                        feedRuntime.setRuntimeState(null);
                    }
                    resumeOldState = false;
                }
                currentBuffer = buffer;
                coreOperatorNodePushable.nextFrame(buffer);
                currentBuffer = null;
            } catch (HyracksDataException e) {
                if (policyEnforcer.getFeedPolicyAccessor().continueOnApplicationFailure()) {
                    boolean isExceptionHarmful = e.getCause() instanceof TreeIndexException && !resumeOldState;
                    if (isExceptionHarmful) {
                        // TODO: log the tuple
                        FeedRuntimeState runtimeState = new FeedRuntimeState(buffer, writer, e);
                        feedRuntime.setRuntimeState(runtimeState);
                    } else {
                        // ignore the frame (exception is expected)
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Ignoring exception " + e);
                        }
                    }
                } else {
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.severe("Feed policy does not require feed to survive soft failure");
                    }
                    throw e;
                }
            }
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
                    feedManager.deRegisterFeedRuntime(feedRuntime.getFeedRuntimeId());
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("No state to save, de-registered feed runtime " + feedRuntime.getFeedRuntimeId());
                    }
                }
            }
            coreOperatorNodePushable.fail();
        }

        @Override
        public void close() throws HyracksDataException {
            coreOperatorNodePushable.close();
            feedManager.deRegisterFeedRuntime(feedRuntime.getFeedRuntimeId());
        }

    }

    public IOperatorDescriptor getCoreOperator() {
        return coreOperator;
    }

}
