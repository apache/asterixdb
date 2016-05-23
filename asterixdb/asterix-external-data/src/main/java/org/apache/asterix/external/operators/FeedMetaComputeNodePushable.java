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
import org.apache.asterix.external.feed.api.IFeedRuntime.FeedRuntimeType;
import org.apache.asterix.external.feed.dataflow.FeedRuntimeInputHandler;
import org.apache.asterix.external.feed.dataflow.SyncFeedRuntimeInputHandler;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.management.FeedManager;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.policy.FeedPolicyEnforcer;
import org.apache.asterix.external.feed.runtime.FeedRuntime;
import org.apache.asterix.external.feed.runtime.FeedRuntimeId;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
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

    /** The (singleton) instance of IFeedManager **/
    private FeedManager feedManager;

    private FrameTupleAccessor fta;

    private final IHyracksTaskContext ctx;

    private final FeedRuntimeType runtimeType = FeedRuntimeType.COMPUTE;

    private final VSizeFrame message;

    private final FeedMetaOperatorDescriptor opDesc;

    private final IRecordDescriptorProvider recordDescProvider;

    private boolean opened;

    /*
     * In this operator:
     * writer is the network partitioner
     * coreOperator is the first operator
     */
    public FeedMetaComputeNodePushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
            int partition, int nPartitions, IOperatorDescriptor coreOperator, FeedConnectionId feedConnectionId,
            Map<String, String> feedPolicyProperties, String operationId,
            FeedMetaOperatorDescriptor feedMetaOperatorDescriptor) throws HyracksDataException {
        this.ctx = ctx;
        this.coreOperator = (AbstractUnaryInputUnaryOutputOperatorNodePushable) ((IActivity) coreOperator)
                .createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
        this.policyEnforcer = new FeedPolicyEnforcer(feedConnectionId, feedPolicyProperties);
        this.partition = partition;
        this.connectionId = feedConnectionId;
        this.feedManager = (FeedManager) ((IAsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext()
                .getApplicationObject()).getFeedManager();
        this.message = new VSizeFrame(ctx);
        ctx.setSharedObject(message);
        this.opDesc = feedMetaOperatorDescriptor;
        this.recordDescProvider = recordDescProvider;
    }

    @Override
    public void open() throws HyracksDataException {
        FeedRuntimeId runtimeId =
                new FeedRuntimeId(connectionId.getFeedId(), runtimeType, partition, FeedRuntimeId.DEFAULT_TARGET_ID);
        try {
            initializeNewFeedRuntime(runtimeId);
            opened = true;
            writer.open();
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    private void initializeNewFeedRuntime(FeedRuntimeId runtimeId) throws Exception {
        fta = new FrameTupleAccessor(recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0));
        FeedPolicyAccessor fpa = policyEnforcer.getFeedPolicyAccessor();
        coreOperator.setOutputFrameWriter(0, writer, recordDesc);
        if (fpa.bufferingEnabled()) {
            writer = new FeedRuntimeInputHandler(ctx, connectionId, runtimeId, coreOperator, fpa, fta,
                    feedManager.getFeedMemoryManager());
        } else {
            writer = new SyncFeedRuntimeInputHandler(ctx, coreOperator, fta);
        }
        feedRuntime = new FeedRuntime(runtimeId);
        feedManager.getFeedConnectionManager().registerFeedRuntime(connectionId, feedRuntime);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        try {
            FeedUtils.processFeedMessage(buffer, message, fta);
            writer.nextFrame(buffer);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, e.getMessage(), e);
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
            deregister();
        } finally {
            if (opened) {
                writer.close();
            }
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
