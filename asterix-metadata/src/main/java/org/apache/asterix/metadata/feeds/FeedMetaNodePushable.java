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
package org.apache.asterix.metadata.feeds;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedRuntime;
import org.apache.asterix.common.feeds.FeedRuntimeId;
import org.apache.asterix.common.feeds.FeedRuntimeInputHandler;
import org.apache.asterix.common.feeds.api.IFeedManager;
import org.apache.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import org.apache.asterix.common.feeds.api.IFeedRuntime.Mode;
import org.apache.asterix.external.feeds.FeedPolicyEnforcer;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class FeedMetaNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private static final Logger LOGGER = Logger.getLogger(FeedMetaNodePushable.class.getName());

    /** Runtime node pushable corresponding to the core feed operator **/
    private AbstractUnaryInputUnaryOutputOperatorNodePushable coreOperator;

    /**
     * A policy enforcer that ensures dyanmic decisions for a feed are taken
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

    /** Total number of partitions available **/
    private int nPartitions;

    /** Type associated with the core feed operator **/
    private final FeedRuntimeType runtimeType = FeedRuntimeType.OTHER;

    /** The (singleton) instance of IFeedManager **/
    private IFeedManager feedManager;

    private FrameTupleAccessor fta;

    private final IHyracksTaskContext ctx;

    private final String operandId;

    /** The pre-processor associated with this runtime **/
    private FeedRuntimeInputHandler inputSideHandler;

    public FeedMetaNodePushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider, int partition,
            int nPartitions, IOperatorDescriptor coreOperator, FeedConnectionId feedConnectionId,
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
        this.operandId = operationId;
    }

    @Override
    public void open() throws HyracksDataException {
        FeedRuntimeId runtimeId = new FeedRuntimeId(runtimeType, partition, operandId);
        try {
            feedRuntime = feedManager.getFeedConnectionManager().getFeedRuntime(connectionId, runtimeId);
            if (feedRuntime == null) {
                initializeNewFeedRuntime(runtimeId);
            } else {
                reviveOldFeedRuntime(runtimeId);
            }
            coreOperator.open();
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    private void initializeNewFeedRuntime(FeedRuntimeId runtimeId) throws Exception {
        this.fta = new FrameTupleAccessor(recordDesc);
        this.inputSideHandler = new FeedRuntimeInputHandler(ctx, connectionId, runtimeId,
                (AbstractUnaryInputUnaryOutputOperatorNodePushable) coreOperator,
                policyEnforcer.getFeedPolicyAccessor(), false, fta, recordDesc, feedManager,
                nPartitions);

        setupBasicRuntime(inputSideHandler);
    }

    private void reviveOldFeedRuntime(FeedRuntimeId runtimeId) throws Exception {
        this.inputSideHandler = feedRuntime.getInputHandler();
        this.fta = new FrameTupleAccessor(recordDesc);
        coreOperator.setOutputFrameWriter(0, writer, recordDesc);
        feedRuntime.setMode(Mode.PROCESS);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Retreived state from the zombie instance " + runtimeType + " node.");
        }
    }

    private void setupBasicRuntime(FeedRuntimeInputHandler inputHandler) throws Exception {
        coreOperator.setOutputFrameWriter(0, writer, recordDesc);
        FeedRuntimeId runtimeId = new FeedRuntimeId(runtimeType, partition, operandId);
        feedRuntime = new FeedRuntime(runtimeId, inputHandler, writer);
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
            LOGGER.info("Core Op:" + coreOperator.getDisplayName() + " fail ");
        }
        feedRuntime.setMode(Mode.FAIL);
        coreOperator.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            coreOperator.close();
        } catch (Exception e) {
            e.printStackTrace();
            // ignore
        } finally {
            if (inputSideHandler != null) {
                inputSideHandler.close();
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Ending Operator  " + this.feedRuntime.getRuntimeId());
            }
        }
    }

}