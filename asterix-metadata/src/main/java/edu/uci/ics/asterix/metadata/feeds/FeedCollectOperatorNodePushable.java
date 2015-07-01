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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.CollectionRuntime;
import edu.uci.ics.asterix.common.feeds.FeedCollectRuntimeInputHandler;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedFrameCollector.State;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedPolicyAccessor;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeInputHandler;
import edu.uci.ics.asterix.common.feeds.SubscribableFeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.api.IFeedManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedOperatorOutputSideHandler;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.Mode;
import edu.uci.ics.asterix.common.feeds.api.ISubscribableRuntime;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * The runtime for @see{FeedIntakeOperationDescriptor}
 */
public class FeedCollectOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

    private static Logger LOGGER = Logger.getLogger(FeedCollectOperatorNodePushable.class.getName());

    private final int partition;
    private final FeedConnectionId connectionId;
    private final Map<String, String> feedPolicy;
    private final FeedPolicyAccessor policyAccessor;
    private final IFeedManager feedManager;
    private final ISubscribableRuntime sourceRuntime;
    private final IHyracksTaskContext ctx;
    private final int nPartitions;

    private RecordDescriptor outputRecordDescriptor;
    private FeedRuntimeInputHandler inputSideHandler;
    private CollectionRuntime collectRuntime;

    public FeedCollectOperatorNodePushable(IHyracksTaskContext ctx, FeedId sourceFeedId,
            FeedConnectionId feedConnectionId, Map<String, String> feedPolicy, int partition, int nPartitions,
            ISubscribableRuntime sourceRuntime) {
        this.ctx = ctx;
        this.partition = partition;
        this.nPartitions = nPartitions;
        this.connectionId = feedConnectionId;
        this.sourceRuntime = sourceRuntime;
        this.feedPolicy = feedPolicy;
        policyAccessor = new FeedPolicyAccessor(feedPolicy);
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.feedManager = runtimeCtx.getFeedManager();
    }

    @Override
    public void initialize() throws HyracksDataException {
        try {
            outputRecordDescriptor = recordDesc;
            FeedRuntimeType sourceRuntimeType = ((SubscribableFeedRuntimeId) sourceRuntime.getRuntimeId())
                    .getFeedRuntimeType();
            switch (sourceRuntimeType) {
                case INTAKE:
                    handleCompleteConnection();
                    break;
                case COMPUTE:
                    handlePartialConnection();
                    break;
                default:
                    throw new IllegalStateException("Invalid source type " + sourceRuntimeType);
            }

            State state = collectRuntime.waitTillCollectionOver();
            if (state.equals(State.FINISHED)) {
                feedManager.getFeedConnectionManager().deRegisterFeedRuntime(connectionId,
                        collectRuntime.getRuntimeId());
                writer.close();
                inputSideHandler.close();
            } else if (state.equals(State.HANDOVER)) {
                inputSideHandler.setMode(Mode.STALL);
                writer.close();
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Ending Collect Operator, the input side handler is now in " + Mode.STALL
                            + " and the output writer " + writer + " has been closed ");
                }
            }
        } catch (InterruptedException ie) {
            handleInterruptedException(ie);
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    private void handleCompleteConnection() throws Exception {
        FeedRuntimeId runtimeId = new FeedRuntimeId(FeedRuntimeType.COLLECT, partition,
                FeedRuntimeId.DEFAULT_OPERAND_ID);
        collectRuntime = (CollectionRuntime) feedManager.getFeedConnectionManager().getFeedRuntime(connectionId,
                runtimeId);
        if (collectRuntime == null) {
            beginNewFeed(runtimeId);
        } else {
            reviveOldFeed();
        }
    }

    private void beginNewFeed(FeedRuntimeId runtimeId) throws Exception {
        writer.open();
        IFrameWriter outputSideWriter = writer;
        if (((SubscribableFeedRuntimeId) sourceRuntime.getRuntimeId()).getFeedRuntimeType().equals(
                FeedRuntimeType.COMPUTE)) {
            outputSideWriter = new CollectTransformFeedFrameWriter(ctx, writer, sourceRuntime, outputRecordDescriptor,
                    connectionId);
            this.recordDesc = sourceRuntime.getRecordDescriptor();
        }

        FrameTupleAccessor tupleAccessor = new FrameTupleAccessor(recordDesc);
        inputSideHandler = new FeedCollectRuntimeInputHandler(ctx, connectionId, runtimeId, outputSideWriter, policyAccessor,
                false,  tupleAccessor, recordDesc,
                feedManager, nPartitions);

        collectRuntime = new CollectionRuntime(connectionId, runtimeId, inputSideHandler, outputSideWriter,
                sourceRuntime, feedPolicy);
        feedManager.getFeedConnectionManager().registerFeedRuntime(connectionId, collectRuntime);
        sourceRuntime.subscribeFeed(policyAccessor, collectRuntime);
    }

    private void reviveOldFeed() throws HyracksDataException {
        writer.open();
        collectRuntime.getFrameCollector().setState(State.ACTIVE);
        inputSideHandler = collectRuntime.getInputHandler();

        IFrameWriter innerWriter = inputSideHandler.getCoreOperator();
        if (innerWriter instanceof CollectTransformFeedFrameWriter) {
            ((CollectTransformFeedFrameWriter) innerWriter).reset(this.writer);
        } else {
            inputSideHandler.setCoreOperator(writer);
        }

        inputSideHandler.setMode(Mode.PROCESS);
    }

    private void handlePartialConnection() throws Exception {
        FeedRuntimeId runtimeId = new FeedRuntimeId(FeedRuntimeType.COMPUTE_COLLECT, partition,
                FeedRuntimeId.DEFAULT_OPERAND_ID);
        writer.open();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Beginning new feed (from existing partial connection):" + connectionId);
        }
        IFeedOperatorOutputSideHandler wrapper = new CollectTransformFeedFrameWriter(ctx, writer, sourceRuntime,
                outputRecordDescriptor, connectionId);

        inputSideHandler = new FeedRuntimeInputHandler(ctx, connectionId, runtimeId, wrapper, policyAccessor, false,
                 new FrameTupleAccessor(recordDesc), recordDesc, feedManager,
                nPartitions);

        collectRuntime = new CollectionRuntime(connectionId, runtimeId, inputSideHandler, wrapper, sourceRuntime,
                feedPolicy);
        feedManager.getFeedConnectionManager().registerFeedRuntime(connectionId, collectRuntime);
        recordDesc = sourceRuntime.getRecordDescriptor();
        sourceRuntime.subscribeFeed(policyAccessor, collectRuntime);
    }

    private void handleInterruptedException(InterruptedException ie) throws HyracksDataException {
        if (policyAccessor.continueOnHardwareFailure()) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Continuing on failure as per feed policy, switching to " + Mode.STALL
                        + " until failure is resolved");
            }
            inputSideHandler.setMode(Mode.STALL);
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Failure during feed ingestion. Deregistering feed runtime " + collectRuntime
                        + " as feed is not configured to handle failures");
            }
            feedManager.getFeedConnectionManager().deRegisterFeedRuntime(connectionId, collectRuntime.getRuntimeId());
            writer.close();
            throw new HyracksDataException(ie);
        }
    }

}
