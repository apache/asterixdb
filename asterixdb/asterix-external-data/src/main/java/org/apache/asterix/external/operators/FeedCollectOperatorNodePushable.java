/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.operators;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.external.feed.api.IFeedManager;
import org.apache.asterix.external.feed.api.IFeedOperatorOutputSideHandler;
import org.apache.asterix.external.feed.api.IFeedRuntime.FeedRuntimeType;
import org.apache.asterix.external.feed.api.IFeedRuntime.Mode;
import org.apache.asterix.external.feed.api.ISubscribableRuntime;
import org.apache.asterix.external.feed.dataflow.CollectTransformFeedFrameWriter;
import org.apache.asterix.external.feed.dataflow.FeedCollectRuntimeInputHandler;
import org.apache.asterix.external.feed.dataflow.FeedFrameCollector.State;
import org.apache.asterix.external.feed.dataflow.FeedRuntimeInputHandler;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.management.FeedId;
import org.apache.asterix.external.feed.message.FeedPartitionStartMessage;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.runtime.CollectionRuntime;
import org.apache.asterix.external.feed.runtime.FeedRuntimeId;
import org.apache.asterix.external.feed.runtime.SubscribableFeedRuntimeId;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * The first operator in a collect job in a feed.
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
        this.policyAccessor = new FeedPolicyAccessor(feedPolicy);
        this.feedManager = (IFeedManager) ((IAsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext()
                .getApplicationObject()).getFeedManager();
    }

    @Override
    public void initialize() throws HyracksDataException {
        try {
            outputRecordDescriptor = recordDesc;
            switch (((SubscribableFeedRuntimeId) sourceRuntime.getRuntimeId()).getFeedRuntimeType()) {
                case INTAKE:
                    handleCompleteConnection();
                    // Notify CC that Collection started
                    ctx.sendApplicationMessageToCC(
                            new FeedPartitionStartMessage(connectionId.getFeedId(), ctx.getJobletContext().getJobId()),
                            null);
                    break;
                case COMPUTE:
                    handlePartialConnection();
                    break;
                default:
                    throw new IllegalStateException("Invalid source type "
                            + ((SubscribableFeedRuntimeId) sourceRuntime.getRuntimeId()).getFeedRuntimeType());
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
        if (((SubscribableFeedRuntimeId) sourceRuntime.getRuntimeId()).getFeedRuntimeType()
                .equals(FeedRuntimeType.COMPUTE)) {
            outputSideWriter = new CollectTransformFeedFrameWriter(ctx, writer, sourceRuntime, outputRecordDescriptor,
                    connectionId);
            this.recordDesc = sourceRuntime.getRecordDescriptor();
        }

        FrameTupleAccessor tupleAccessor = new FrameTupleAccessor(recordDesc);
        inputSideHandler = new FeedCollectRuntimeInputHandler(ctx, connectionId, runtimeId, outputSideWriter,
                policyAccessor, false, tupleAccessor, recordDesc, feedManager, nPartitions);

        collectRuntime = new CollectionRuntime(connectionId, runtimeId, inputSideHandler, outputSideWriter,
                sourceRuntime, feedPolicy, ctx);
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
                new FrameTupleAccessor(recordDesc), recordDesc, feedManager, nPartitions);

        collectRuntime = new CollectionRuntime(connectionId, runtimeId, inputSideHandler, wrapper, sourceRuntime,
                feedPolicy, ctx);
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
