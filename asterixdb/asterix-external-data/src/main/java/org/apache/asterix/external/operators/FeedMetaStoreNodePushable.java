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
import org.apache.asterix.external.feed.api.IFeedRuntime.FeedRuntimeType;
import org.apache.asterix.external.feed.dataflow.FeedRuntimeInputHandler;
import org.apache.asterix.external.feed.dataflow.SyncFeedRuntimeInputHandler;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.management.FeedManager;
import org.apache.asterix.external.feed.policy.FeedPolicyEnforcer;
import org.apache.asterix.external.feed.runtime.FeedRuntime;
import org.apache.asterix.external.feed.runtime.FeedRuntimeId;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.hyracks.api.comm.IFrameWriter;
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
    private AbstractUnaryInputUnaryOutputOperatorNodePushable insertOperator;

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

    /** Type associated with the core feed operator **/
    private final FeedRuntimeType runtimeType = FeedRuntimeType.STORE;

    /** The (singleton) instance of IFeedManager **/
    private final FeedManager feedManager;

    private FrameTupleAccessor fta;

    private final IHyracksTaskContext ctx;

    private final String targetId;

    private final ByteBuffer message = ByteBuffer.allocate(MessagingFrameTupleAppender.MAX_MESSAGE_SIZE);

    private final IRecordDescriptorProvider recordDescProvider;

    private final FeedMetaOperatorDescriptor opDesc;

    public FeedMetaStoreNodePushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
            int partition, int nPartitions, IOperatorDescriptor coreOperator, FeedConnectionId feedConnectionId,
            Map<String, String> feedPolicyProperties, String targetId,
            FeedMetaOperatorDescriptor feedMetaOperatorDescriptor) throws HyracksDataException {
        this.ctx = ctx;
        this.insertOperator = (AbstractUnaryInputUnaryOutputOperatorNodePushable) ((IActivity) coreOperator)
                .createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
        this.policyEnforcer = new FeedPolicyEnforcer(feedConnectionId, feedPolicyProperties);
        this.partition = partition;
        this.connectionId = feedConnectionId;
        this.feedManager = (FeedManager) ((IAsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext()
                .getApplicationObject()).getFeedManager();
        this.targetId = targetId;
        ctx.setSharedObject(message);
        this.recordDescProvider = recordDescProvider;
        this.opDesc = feedMetaOperatorDescriptor;
    }

    @Override
    public void open() throws HyracksDataException {
        FeedRuntimeId runtimeId = new FeedRuntimeId(connectionId.getFeedId(), runtimeType, partition, targetId);
        try {
            initializeNewFeedRuntime(runtimeId);
            insertOperator.open();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to open feed store operator", e);
            throw new HyracksDataException(e);
        }
    }

    private void initializeNewFeedRuntime(FeedRuntimeId runtimeId) throws Exception {
        fta = new FrameTupleAccessor(recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0));
        insertOperator.setOutputFrameWriter(0, writer, recordDesc);
        if (insertOperator instanceof AsterixLSMInsertDeleteOperatorNodePushable) {
            AsterixLSMInsertDeleteOperatorNodePushable indexOp =
                    (AsterixLSMInsertDeleteOperatorNodePushable) insertOperator;
            if (!indexOp.isPrimary()) {
                writer = insertOperator;
                setupBasicRuntime(writer);
                return;
            }
        }
        if (policyEnforcer.getFeedPolicyAccessor().bufferingEnabled()) {
            writer = new FeedRuntimeInputHandler(ctx, connectionId, runtimeId, insertOperator,
                    policyEnforcer.getFeedPolicyAccessor(), fta, feedManager.getFeedMemoryManager());
        } else {
            writer = new SyncFeedRuntimeInputHandler(ctx, insertOperator, fta);
        }
        setupBasicRuntime(writer);
    }

    private void setupBasicRuntime(IFrameWriter frameWriter) throws Exception {
        FeedRuntimeId runtimeId = new FeedRuntimeId(connectionId.getFeedId(), runtimeType, partition, targetId);
        feedRuntime = new FeedRuntime(runtimeId);
        feedManager.getFeedConnectionManager().registerFeedRuntime(connectionId, feedRuntime);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        try {
            FeedUtils.processFeedMessage(buffer, message, fta);
            writer.nextFrame(buffer);
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            writer.close();
        } finally {
            deregister();
        }
    }

    private void deregister() {
        feedManager.getFeedConnectionManager().deRegisterFeedRuntime(connectionId, feedRuntime.getRuntimeId());
    }

    @Override
    public void flush() throws HyracksDataException {
        writer.flush();
    }

}
