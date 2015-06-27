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
package edu.uci.ics.asterix.metadata.feeds;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.CollectionRuntime;
import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter;
import edu.uci.ics.asterix.common.feeds.FeedCollectRuntimeInputHandler;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedFrameCollector;
import edu.uci.ics.asterix.common.feeds.FeedFrameCollector.State;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedRuntime;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeInputHandler;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeManager;
import edu.uci.ics.asterix.common.feeds.FeedTupleCommitResponseMessage;
import edu.uci.ics.asterix.common.feeds.IngestionRuntime;
import edu.uci.ics.asterix.common.feeds.IntakePartitionStatistics;
import edu.uci.ics.asterix.common.feeds.MonitoredBufferTimerTasks.MonitoredBufferStorageTimerTask;
import edu.uci.ics.asterix.common.feeds.StorageSideMonitoredBuffer;
import edu.uci.ics.asterix.common.feeds.SubscribableFeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.api.IAdapterRuntimeManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedMessage;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.Mode;
import edu.uci.ics.asterix.common.feeds.api.IIntakeProgressTracker;
import edu.uci.ics.asterix.common.feeds.api.ISubscribableRuntime;
import edu.uci.ics.asterix.common.feeds.message.EndFeedMessage;
import edu.uci.ics.asterix.common.feeds.message.ThrottlingEnabledFeedMessage;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * Runtime for the FeedMessageOpertorDescriptor. This operator is responsible for communicating
 * a feed message to the local feed manager on the host node controller.
 * 
 * @see FeedMessageOperatorDescriptor
 *      IFeedMessage
 *      IFeedManager
 */
public class FeedMessageOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

    private static final Logger LOGGER = Logger.getLogger(FeedMessageOperatorNodePushable.class.getName());

    private final FeedConnectionId connectionId;
    private final IFeedMessage message;
    private final IFeedManager feedManager;
    private final int partition;

    public FeedMessageOperatorNodePushable(IHyracksTaskContext ctx, FeedConnectionId connectionId,
            IFeedMessage feedMessage, int partition, int nPartitions) {
        this.connectionId = connectionId;
        this.message = feedMessage;
        this.partition = partition;
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.feedManager = runtimeCtx.getFeedManager();
    }

    @Override
    public void initialize() throws HyracksDataException {
        try {
            writer.open();
            switch (message.getMessageType()) {
                case END:
                    EndFeedMessage endFeedMessage = (EndFeedMessage) message;
                    switch (endFeedMessage.getEndMessageType()) {
                        case DISCONNECT_FEED:
                            hanldeDisconnectFeedTypeMessage(endFeedMessage);
                            break;
                        case DISCONTINUE_SOURCE:
                            handleDiscontinueFeedTypeMessage(endFeedMessage);
                            break;
                    }
                    break;
                case PREPARE_STALL: {
                    handlePrepareStallMessage((PrepareStallMessage) message);
                    break;
                }
                case TERMINATE_FLOW: {
                    FeedConnectionId connectionId = ((TerminateDataFlowMessage) message).getConnectionId();
                    handleTerminateFlowMessage(connectionId);
                    break;
                }
                case COMMIT_ACK_RESPONSE: {
                    handleFeedTupleCommitResponseMessage((FeedTupleCommitResponseMessage) message);
                    break;
                }
                case THROTTLING_ENABLED: {
                    handleThrottlingEnabledMessage((ThrottlingEnabledFeedMessage) message);
                    break;
                }
                default:
                    break;

            }

        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            writer.close();
        }
    }

    private void handleThrottlingEnabledMessage(ThrottlingEnabledFeedMessage throttlingMessage) {
        FeedConnectionId connectionId = throttlingMessage.getConnectionId();
        FeedRuntimeManager runtimeManager = feedManager.getFeedConnectionManager().getFeedRuntimeManager(connectionId);
        Set<FeedRuntimeId> runtimes = runtimeManager.getFeedRuntimes();
        for (FeedRuntimeId runtimeId : runtimes) {
            if (runtimeId.getFeedRuntimeType().equals(FeedRuntimeType.STORE)) {
                FeedRuntime storeRuntime = runtimeManager.getFeedRuntime(runtimeId);
                ((StorageSideMonitoredBuffer) (storeRuntime.getInputHandler().getmBuffer())).setAcking(false);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Acking Disabled in view of throttling that has been activted upfron in the pipeline "
                            + connectionId);
                }
            }
        }
    }

    private void handleFeedTupleCommitResponseMessage(FeedTupleCommitResponseMessage commitResponseMessage) {
        FeedConnectionId connectionId = commitResponseMessage.getConnectionId();
        FeedRuntimeManager runtimeManager = feedManager.getFeedConnectionManager().getFeedRuntimeManager(connectionId);
        Set<FeedRuntimeId> runtimes = runtimeManager.getFeedRuntimes();
        for (FeedRuntimeId runtimeId : runtimes) {
            FeedRuntime runtime = runtimeManager.getFeedRuntime(runtimeId);
            switch (runtimeId.getFeedRuntimeType()) {
                case COLLECT:
                    FeedCollectRuntimeInputHandler inputHandler = (FeedCollectRuntimeInputHandler) runtime
                            .getInputHandler();
                    int maxBasePersisted = commitResponseMessage.getMaxWindowAcked();
                    inputHandler.dropTill(IntakePartitionStatistics.ACK_WINDOW_SIZE * (maxBasePersisted + 1));
                    break;
                case STORE:
                    MonitoredBufferStorageTimerTask sTask = runtime.getInputHandler().getmBuffer()
                            .getStorageTimeTrackingRateTask();
                    sTask.receiveCommitAckResponse(commitResponseMessage);
                    break;
            }
        }

        commitResponseMessage.getIntakePartition();
        SubscribableFeedRuntimeId sid = new SubscribableFeedRuntimeId(connectionId.getFeedId(), FeedRuntimeType.INTAKE,
                partition);
        IngestionRuntime ingestionRuntime = (IngestionRuntime) feedManager.getFeedSubscriptionManager()
                .getSubscribableRuntime(sid);
        if (ingestionRuntime != null) {
            IIntakeProgressTracker tracker = ingestionRuntime.getAdapterRuntimeManager().getProgressTracker();
            if (tracker != null) {
                tracker.notifyIngestedTupleTimestamp(System.currentTimeMillis());
            }
        }
    }

    private void handleTerminateFlowMessage(FeedConnectionId connectionId) throws HyracksDataException {
        FeedRuntimeManager runtimeManager = feedManager.getFeedConnectionManager().getFeedRuntimeManager(connectionId);
        Set<FeedRuntimeId> feedRuntimes = runtimeManager.getFeedRuntimes();

        boolean found = false;
        for (FeedRuntimeId runtimeId : feedRuntimes) {
            FeedRuntime runtime = runtimeManager.getFeedRuntime(runtimeId);
            if (runtime.getRuntimeId().getRuntimeType().equals(FeedRuntimeType.COLLECT)) {
                ((CollectionRuntime) runtime).getFrameCollector().setState(State.HANDOVER);
                found = true;
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Switched " + runtime + " to Hand Over stage");
                }
            }
        }
        if (!found) {
            throw new HyracksDataException("COLLECT Runtime  not found!");
        }
    }

    private void handlePrepareStallMessage(PrepareStallMessage prepareStallMessage) throws HyracksDataException {
        FeedConnectionId connectionId = prepareStallMessage.getConnectionId();
        int computePartitionsRetainLimit = prepareStallMessage.getComputePartitionsRetainLimit();
        FeedRuntimeManager runtimeManager = feedManager.getFeedConnectionManager().getFeedRuntimeManager(connectionId);
        Set<FeedRuntimeId> feedRuntimes = runtimeManager.getFeedRuntimes();
        for (FeedRuntimeId runtimeId : feedRuntimes) {
            FeedRuntime runtime = runtimeManager.getFeedRuntime(runtimeId);
            switch (runtimeId.getFeedRuntimeType()) {
                case COMPUTE:
                    Mode requiredMode = runtimeId.getPartition() <= computePartitionsRetainLimit ? Mode.STALL
                            : Mode.END;
                    runtime.setMode(requiredMode);
                    break;
                default:
                    runtime.setMode(Mode.STALL);
                    break;
            }
        }
    }

    private void handleDiscontinueFeedTypeMessage(EndFeedMessage endFeedMessage) throws Exception {
        FeedId sourceFeedId = endFeedMessage.getSourceFeedId();
        SubscribableFeedRuntimeId subscribableRuntimeId = new SubscribableFeedRuntimeId(sourceFeedId,
                FeedRuntimeType.INTAKE, partition);
        ISubscribableRuntime feedRuntime = feedManager.getFeedSubscriptionManager().getSubscribableRuntime(
                subscribableRuntimeId);
        IAdapterRuntimeManager adapterRuntimeManager = ((IngestionRuntime) feedRuntime).getAdapterRuntimeManager();
        adapterRuntimeManager.stop();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Stopped Adapter " + adapterRuntimeManager);
        }
    }

    private void hanldeDisconnectFeedTypeMessage(EndFeedMessage endFeedMessage) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Ending feed:" + endFeedMessage.getFeedConnectionId());
        }
        FeedRuntimeId runtimeId = null;
        FeedRuntimeType subscribableRuntimeType = ((EndFeedMessage) message).getSourceRuntimeType();
        if (endFeedMessage.isCompleteDisconnection()) {
            // subscribableRuntimeType represents the location at which the feed connection receives data
            FeedRuntimeType runtimeType = null;
            switch (subscribableRuntimeType) {
                case INTAKE:
                    runtimeType = FeedRuntimeType.COLLECT;
                    break;
                case COMPUTE:
                    runtimeType = FeedRuntimeType.COMPUTE_COLLECT;
                    break;
                default:
                    throw new IllegalStateException("Invalid subscribable runtime type " + subscribableRuntimeType);
            }

            runtimeId = new FeedRuntimeId(runtimeType, partition, FeedRuntimeId.DEFAULT_OPERAND_ID);
            CollectionRuntime feedRuntime = (CollectionRuntime) feedManager.getFeedConnectionManager().getFeedRuntime(
                    connectionId, runtimeId);
            feedRuntime.getSourceRuntime().unsubscribeFeed(feedRuntime);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Complete Unsubscription of " + endFeedMessage.getFeedConnectionId());
            }
        } else {
            // subscribaleRuntimeType represents the location for data hand-off in presence of subscribers
            switch (subscribableRuntimeType) {
                case INTAKE:
                    // illegal state as data hand-off from one feed to another does not happen at intake
                    throw new IllegalStateException("Illegal State, invalid runtime type  " + subscribableRuntimeType);
                case COMPUTE:
                    // feed could be primary or secondary, doesn't matter
                    SubscribableFeedRuntimeId feedSubscribableRuntimeId = new SubscribableFeedRuntimeId(
                            connectionId.getFeedId(), FeedRuntimeType.COMPUTE, partition);
                    ISubscribableRuntime feedRuntime = feedManager.getFeedSubscriptionManager().getSubscribableRuntime(
                            feedSubscribableRuntimeId);
                    DistributeFeedFrameWriter dWriter = (DistributeFeedFrameWriter) feedRuntime.getFeedFrameWriter();
                    Map<IFrameWriter, FeedFrameCollector> registeredCollectors = dWriter.getRegisteredReaders();

                    IFrameWriter unsubscribingWriter = null;
                    for (Entry<IFrameWriter, FeedFrameCollector> entry : registeredCollectors.entrySet()) {
                        IFrameWriter frameWriter = entry.getKey();
                        FeedRuntimeInputHandler feedFrameWriter = (FeedRuntimeInputHandler) frameWriter;
                        if (feedFrameWriter.getConnectionId().equals(endFeedMessage.getFeedConnectionId())) {
                            unsubscribingWriter = feedFrameWriter;
                            dWriter.unsubscribeFeed(unsubscribingWriter);
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("Partial Unsubscription of " + unsubscribingWriter);
                            }
                            break;
                        }
                    }
                    break;
            }

        }

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Unsubscribed from feed :" + connectionId);
        }
    }
}
