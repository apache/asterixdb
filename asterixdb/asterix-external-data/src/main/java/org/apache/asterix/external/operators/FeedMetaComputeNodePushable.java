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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.external.feed.api.IFeedManager;
import org.apache.asterix.external.feed.api.IFeedRuntime.FeedRuntimeType;
import org.apache.asterix.external.feed.api.IFeedRuntime.Mode;
import org.apache.asterix.external.feed.api.ISubscribableRuntime;
import org.apache.asterix.external.feed.dataflow.DistributeFeedFrameWriter;
import org.apache.asterix.external.feed.dataflow.FeedRuntimeInputHandler;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.policy.FeedPolicyEnforcer;
import org.apache.asterix.external.feed.runtime.FeedRuntime;
import org.apache.asterix.external.feed.runtime.FeedRuntimeId;
import org.apache.asterix.external.feed.runtime.SubscribableFeedRuntimeId;
import org.apache.asterix.external.feed.runtime.SubscribableRuntime;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.io.MessagingFrameTupleAppender;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

/*
 * This IFrameWriter doesn't follow the contract
 */
public class FeedMetaComputeNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private static final Logger LOGGER = Logger.getLogger(FeedMetaComputeNodePushable.class.getName());

    /** Runtime node pushable corresponding to the core feed operator **/
    private AbstractUnaryInputUnaryOutputOperatorNodePushable coreOperator;

    /**
     * A policy enforcer that ensures dynamic decisions for a feed are taken
     * in accordance with the associated ingestion policy
     **/
    private FeedPolicyEnforcer policyEnforcer;

    /**
     * The Feed Runtime instance associated with the operator. Feed Runtime
     * captures the state of the operator while the feed is active.
     */
    private FeedRuntime feedRuntime;

    /**
     * A unique identifier for the feed instance. A feed instance represents
     * the flow of data from a feed to a dataset.
     **/
    private FeedConnectionId connectionId;

    /**
     * Denotes the i'th operator instance in a setting where K operator
     * instances are scheduled to run in parallel
     **/
    private int partition;

    private int nPartitions;

    /** The (singleton) instance of IFeedManager **/
    private IFeedManager feedManager;

    private FrameTupleAccessor fta;

    private final IHyracksTaskContext ctx;

    private final FeedRuntimeType runtimeType = FeedRuntimeType.COMPUTE;

    private FeedRuntimeInputHandler inputSideHandler;

    private ByteBuffer message = ByteBuffer.allocate(MessagingFrameTupleAppender.MAX_MESSAGE_SIZE);

    private final FeedMetaOperatorDescriptor opDesc;

    private final IRecordDescriptorProvider recordDescProvider;

    public FeedMetaComputeNodePushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
            int partition, int nPartitions, IOperatorDescriptor coreOperator, FeedConnectionId feedConnectionId,
            Map<String, String> feedPolicyProperties, String operationId,
            FeedMetaOperatorDescriptor feedMetaOperatorDescriptor) throws HyracksDataException {
        this.ctx = ctx;
        this.coreOperator = (AbstractUnaryInputUnaryOutputOperatorNodePushable) ((IActivity) coreOperator)
                .createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
        this.policyEnforcer = new FeedPolicyEnforcer(feedConnectionId, feedPolicyProperties);
        this.partition = partition;
        this.nPartitions = nPartitions;
        this.connectionId = feedConnectionId;
        this.feedManager = (IFeedManager) ((IAsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext()
                .getApplicationObject()).getFeedManager();
        ctx.setSharedObject(message);
        this.opDesc = feedMetaOperatorDescriptor;
        this.recordDescProvider = recordDescProvider;
    }

    @Override
    public void open() throws HyracksDataException {
        FeedRuntimeId runtimeId = new SubscribableFeedRuntimeId(connectionId.getFeedId(), runtimeType, partition);
        try {
            feedRuntime = feedManager.getFeedConnectionManager().getFeedRuntime(connectionId, runtimeId);
            if (feedRuntime == null) {
                initializeNewFeedRuntime(runtimeId);
            } else {
                reviveOldFeedRuntime(runtimeId);
            }
            writer.open();
            coreOperator.open();
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    private void initializeNewFeedRuntime(FeedRuntimeId runtimeId) throws Exception {
        this.fta = new FrameTupleAccessor(recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0));
        this.inputSideHandler = new FeedRuntimeInputHandler(ctx, connectionId, runtimeId, coreOperator,
                policyEnforcer.getFeedPolicyAccessor(), policyEnforcer.getFeedPolicyAccessor().bufferingEnabled(), fta,
                recordDesc, feedManager, nPartitions);

        DistributeFeedFrameWriter distributeWriter = new DistributeFeedFrameWriter(ctx, connectionId.getFeedId(),
                writer, runtimeType, partition, new FrameTupleAccessor(recordDesc), feedManager);
        coreOperator.setOutputFrameWriter(0, distributeWriter, recordDesc);

        feedRuntime = new SubscribableRuntime(connectionId.getFeedId(), runtimeId, inputSideHandler, distributeWriter,
                recordDesc);
        feedManager.getFeedSubscriptionManager().registerFeedSubscribableRuntime((ISubscribableRuntime) feedRuntime);
        feedManager.getFeedConnectionManager().registerFeedRuntime(connectionId, feedRuntime);

        distributeWriter.subscribeFeed(policyEnforcer.getFeedPolicyAccessor(), writer, connectionId);
    }

    private void reviveOldFeedRuntime(FeedRuntimeId runtimeId) throws Exception {
        this.fta = new FrameTupleAccessor(recordDesc);
        this.inputSideHandler = feedRuntime.getInputHandler();
        this.inputSideHandler.setCoreOperator(coreOperator);

        DistributeFeedFrameWriter distributeWriter = new DistributeFeedFrameWriter(ctx, connectionId.getFeedId(),
                writer, runtimeType, partition, new FrameTupleAccessor(recordDesc), feedManager);
        coreOperator.setOutputFrameWriter(0, distributeWriter, recordDesc);
        distributeWriter.subscribeFeed(policyEnforcer.getFeedPolicyAccessor(), writer, connectionId);

        inputSideHandler.reset(nPartitions);
        feedRuntime.setMode(Mode.PROCESS);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        try {
            FeedUtils.processFeedMessage(buffer, message, fta);
            inputSideHandler.nextFrame(buffer);
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.warning("Core Op:" + coreOperator.getDisplayName() + " fail ");
        }
        feedRuntime.setMode(Mode.FAIL);
        coreOperator.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        boolean stalled = inputSideHandler.getMode().equals(Mode.STALL);
        boolean end = inputSideHandler.getMode().equals(Mode.END);
        try {
            if (!(stalled || end)) {
                inputSideHandler.nextFrame(null); // signal end of data
                while (!inputSideHandler.isFinished()) {
                    synchronized (coreOperator) {
                        if (inputSideHandler.isFinished()) {
                            break;
                        }
                        coreOperator.wait();
                    }
                }
            } else {
                inputSideHandler.setFinished(true);
            }
            coreOperator.close();
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("CLOSED " + coreOperator + " STALLED ?" + stalled + " ENDED " + end);
            }
        } catch (InterruptedException e) {
            throw new HyracksDataException(e);
        } finally {
            if (!stalled) {
                deregister();
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine("DEREGISTERING " + this.feedRuntime.getRuntimeId());
                }
            } else {
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine("NOT DEREGISTERING " + this.feedRuntime.getRuntimeId());
                }
            }
            inputSideHandler.close();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Ending Operator  " + this.feedRuntime.getRuntimeId());
            }
        }
    }

    private void deregister() {
        if (feedRuntime != null) {
            // deregister from subscription manager
            SubscribableFeedRuntimeId runtimeId = (SubscribableFeedRuntimeId) feedRuntime.getRuntimeId();
            feedManager.getFeedSubscriptionManager().deregisterFeedSubscribableRuntime(runtimeId);

            // deregister from connection manager
            feedManager.getFeedConnectionManager().deRegisterFeedRuntime(connectionId, feedRuntime.getRuntimeId());
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        inputSideHandler.flush();
    }

}
