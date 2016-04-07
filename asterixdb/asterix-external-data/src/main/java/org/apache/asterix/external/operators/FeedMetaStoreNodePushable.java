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
import org.apache.asterix.common.dataflow.AsterixLSMInsertDeleteOperatorNodePushable;
import org.apache.asterix.external.feed.api.IFeedManager;
import org.apache.asterix.external.feed.api.IFeedRuntime.FeedRuntimeType;
import org.apache.asterix.external.feed.api.IFeedRuntime.Mode;
import org.apache.asterix.external.feed.dataflow.FeedRuntimeInputHandler;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.policy.FeedPolicyEnforcer;
import org.apache.asterix.external.feed.runtime.FeedRuntime;
import org.apache.asterix.external.feed.runtime.FeedRuntimeId;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.io.MessagingFrameTupleAppender;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class FeedMetaStoreNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private static final Logger LOGGER = Logger.getLogger(FeedMetaStoreNodePushable.class.getName());

    /** Runtime node pushable corresponding to the core feed operator **/
    private final AbstractUnaryInputUnaryOutputOperatorNodePushable coreOperator;

    /**
     * A policy enforcer that ensures dyanmic decisions for a feed are taken
     * in accordance with the associated ingestion policy
     **/
    private final FeedPolicyEnforcer policyEnforcer;

    /**
     * The Feed Runtime instance associated with the operator. Feed Runtime
     * captures the state of the operator while the feed is active.
     */
    private FeedRuntime feedRuntime;

    /**
     * A unique identifier for the feed instance. A feed instance represents
     * the flow of data from a feed to a dataset.
     **/
    private final FeedConnectionId connectionId;

    /**
     * Denotes the i'th operator instance in a setting where K operator
     * instances are scheduled to run in parallel
     **/
    private final int partition;

    private final int nPartitions;

    /** Type associated with the core feed operator **/
    private final FeedRuntimeType runtimeType = FeedRuntimeType.STORE;

    /** The (singleton) instance of IFeedManager **/
    private final IFeedManager feedManager;

    private FrameTupleAccessor fta;

    private final IHyracksTaskContext ctx;

    private final String operandId;

    private FeedRuntimeInputHandler inputSideHandler;

    private final ByteBuffer message = ByteBuffer.allocate(MessagingFrameTupleAppender.MAX_MESSAGE_SIZE);

    private final IRecordDescriptorProvider recordDescProvider;

    private final FeedMetaOperatorDescriptor opDesc;

    public FeedMetaStoreNodePushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
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
        this.operandId = operationId;
        ctx.setSharedObject(message);
        this.recordDescProvider = recordDescProvider;
        this.opDesc = feedMetaOperatorDescriptor;
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
            LOGGER.log(Level.WARNING, "Failed to open feed store operator", e);
            throw new HyracksDataException(e);
        }
    }

    private void initializeNewFeedRuntime(FeedRuntimeId runtimeId) throws Exception {
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.warning("Runtime not found for  " + runtimeId + " connection id " + connectionId);
        }
        this.fta = new FrameTupleAccessor(recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0));
        this.inputSideHandler = new FeedRuntimeInputHandler(ctx, connectionId, runtimeId, coreOperator,
                policyEnforcer.getFeedPolicyAccessor(), policyEnforcer.getFeedPolicyAccessor().bufferingEnabled(), fta,
                recordDesc, feedManager, nPartitions);
        if (coreOperator instanceof AsterixLSMInsertDeleteOperatorNodePushable) {
            AsterixLSMInsertDeleteOperatorNodePushable indexOp = (AsterixLSMInsertDeleteOperatorNodePushable) coreOperator;
            if (!indexOp.isPrimary()) {
                inputSideHandler.setBufferingEnabled(false);
            }
        }
        setupBasicRuntime(inputSideHandler);
    }

    private void reviveOldFeedRuntime(FeedRuntimeId runtimeId) throws Exception {
        this.inputSideHandler = feedRuntime.getInputHandler();
        this.fta = new FrameTupleAccessor(recordDesc);
        coreOperator.setOutputFrameWriter(0, writer, recordDesc);
        this.inputSideHandler.reset(nPartitions);
        this.inputSideHandler.setCoreOperator(coreOperator);
        feedRuntime.setMode(Mode.PROCESS);
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.warning(
                    "Retreived state from the zombie instance from previous execution for " + runtimeType + " node.");
        }
    }

    private void setupBasicRuntime(FeedRuntimeInputHandler inputHandler) throws Exception {
        coreOperator.setOutputFrameWriter(0, writer, recordDesc);
        FeedRuntimeId runtimeId = new FeedRuntimeId(runtimeType, partition, operandId);
        feedRuntime = new FeedRuntime(runtimeId, inputHandler, writer);
        feedManager.getFeedConnectionManager().registerFeedRuntime(connectionId, feedRuntime);
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
            LOGGER.info("Core Op:" + coreOperator.getDisplayName() + " fail ");
        }
        feedRuntime.setMode(Mode.FAIL);
        coreOperator.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("CLOSE CALLED FOR " + this.feedRuntime.getRuntimeId());
        }
        boolean stalled = inputSideHandler.getMode().equals(Mode.STALL);
        try {
            if (!stalled) {
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine("SIGNALLING END OF DATA for " + this.feedRuntime.getRuntimeId() + " mode is "
                            + inputSideHandler.getMode() + " WAITING ON " + coreOperator);
                }
                inputSideHandler.nextFrame(null); // signal end of data
                while (!inputSideHandler.isFinished()) {
                    synchronized (coreOperator) {
                        if (inputSideHandler.isFinished()) {
                            break;
                        }
                        coreOperator.wait();
                    }
                }
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine("ABOUT TO CLOSE OPERATOR  " + coreOperator);
                }
            }
            coreOperator.close();
        } catch (Exception e) {
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
            feedManager.getFeedConnectionManager().deRegisterFeedRuntime(connectionId, feedRuntime.getRuntimeId());
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        inputSideHandler.flush();
    }

}
