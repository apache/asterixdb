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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedRuntime;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeInputHandler;
import edu.uci.ics.asterix.common.feeds.SubscribableFeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.SubscribableRuntime;
import edu.uci.ics.asterix.common.feeds.api.IFeedManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.Mode;
import edu.uci.ics.asterix.common.feeds.api.ISubscribableRuntime;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

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

    public FeedMetaComputeNodePushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
            int partition, int nPartitions, IOperatorDescriptor coreOperator, FeedConnectionId feedConnectionId,
            Map<String, String> feedPolicyProperties, String operationId) throws HyracksDataException {
        this.ctx = ctx;
        this.coreOperator = (AbstractUnaryInputUnaryOutputOperatorNodePushable) ((IActivity) coreOperator)
                .createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
        this.policyEnforcer = new FeedPolicyEnforcer(feedConnectionId, feedPolicyProperties);
        this.partition = partition;
        this.nPartitions = nPartitions;
        this.connectionId = feedConnectionId;
        this.feedManager = ((IAsterixAppRuntimeContext) (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject()).getFeedManager();
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.feedManager = runtimeCtx.getFeedManager();
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
        this.fta = new FrameTupleAccessor(recordDesc);
        this.inputSideHandler = new FeedRuntimeInputHandler(ctx, connectionId, runtimeId, coreOperator,
                policyEnforcer.getFeedPolicyAccessor(), true, fta, recordDesc, feedManager,
                nPartitions);

        DistributeFeedFrameWriter distributeWriter = new DistributeFeedFrameWriter(ctx, connectionId.getFeedId(), writer,
                runtimeType, partition, new FrameTupleAccessor(recordDesc), feedManager);
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

        DistributeFeedFrameWriter distributeWriter = new DistributeFeedFrameWriter(ctx, connectionId.getFeedId(), writer,
                runtimeType, partition, new FrameTupleAccessor(recordDesc), feedManager);
        coreOperator.setOutputFrameWriter(0, distributeWriter, recordDesc);
        distributeWriter.subscribeFeed(policyEnforcer.getFeedPolicyAccessor(), writer, connectionId);

        inputSideHandler.reset(nPartitions);
        feedRuntime.setMode(Mode.PROCESS);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        try {
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
            if (inputSideHandler != null) {
                if (!(stalled || end)) {
                    inputSideHandler.nextFrame(null); // signal end of data
                    while (!inputSideHandler.isFinished()) {
                        synchronized (coreOperator) {
                            coreOperator.wait();
                        }
                    }
                } else {
                    inputSideHandler.setFinished(true);
                }
            }
            coreOperator.close();
            System.out.println("CLOSED " + coreOperator + " STALLED ?" + stalled + " ENDED " + end);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (!stalled) {
                deregister();
                System.out.println("DEREGISTERING " + this.feedRuntime.getRuntimeId());
            } else {
                System.out.println("NOT DEREGISTERING " + this.feedRuntime.getRuntimeId());
            }
            if (inputSideHandler != null) {
                inputSideHandler.close();
            }
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
            feedManager.getFeedConnectionManager().deRegisterFeedRuntime(connectionId,
                    ((FeedRuntime) feedRuntime).getRuntimeId());
        }
    }

}