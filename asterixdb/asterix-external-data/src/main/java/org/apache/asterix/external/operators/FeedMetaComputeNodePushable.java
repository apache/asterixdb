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

import org.apache.asterix.active.ActiveManager;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.external.feed.dataflow.FeedRuntimeInputHandler;
import org.apache.asterix.external.feed.dataflow.SyncFeedRuntimeInputHandler;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
 * This IFrameWriter doesn't follow the contract
 */
public class FeedMetaComputeNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private static final Logger LOGGER = LogManager.getLogger();

    /** Runtime node pushable corresponding to the core feed operator **/
    private AbstractUnaryInputUnaryOutputOperatorNodePushable coreOperator;

    /**
     * A policy accessor that ensures dynamic decisions for a feed are taken
     * in accordance with the associated ingestion policy
     **/
    private FeedPolicyAccessor policyAccessor;

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
    private ActiveManager feedManager;

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
            Map<String, String> feedPolicyProperties, FeedMetaOperatorDescriptor feedMetaOperatorDescriptor)
            throws HyracksDataException {
        this.ctx = ctx;
        this.coreOperator = (AbstractUnaryInputUnaryOutputOperatorNodePushable) ((IActivity) coreOperator)
                .createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
        this.policyAccessor = new FeedPolicyAccessor(feedPolicyProperties);
        this.partition = partition;
        this.connectionId = feedConnectionId;
        this.feedManager = (ActiveManager) ((INcApplicationContext) ctx.getJobletContext().getServiceContext()
                .getApplicationContext()).getActiveManager();
        this.message = new VSizeFrame(ctx);
        TaskUtil.put(HyracksConstants.KEY_MESSAGE, message, ctx);
        this.opDesc = feedMetaOperatorDescriptor;
        this.recordDescProvider = recordDescProvider;
    }

    @Override
    public void open() throws HyracksDataException {
        ActiveRuntimeId runtimeId = new ActiveRuntimeId(connectionId.getFeedId(), runtimeType.toString(), partition);
        try {
            initializeNewFeedRuntime(runtimeId);
            opened = true;
            writer.open();
        } catch (Exception e) {
            e.printStackTrace();
            throw HyracksDataException.create(e);
        }
    }

    private void initializeNewFeedRuntime(ActiveRuntimeId runtimeId) throws Exception {
        fta = new FrameTupleAccessor(recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0));
        FeedPolicyAccessor fpa = policyAccessor;
        coreOperator.setOutputFrameWriter(0, writer, recordDesc);
        if (fpa.flowControlEnabled()) {
            writer = new FeedRuntimeInputHandler(ctx, connectionId, runtimeId, coreOperator, fpa, fta,
                    feedManager.getFramePool());
        } else {
            writer = new SyncFeedRuntimeInputHandler(ctx, coreOperator, fta);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        try {
            FeedUtils.processFeedMessage(buffer, message, fta);
            writer.nextFrame(buffer);
        } catch (Exception e) {
            LOGGER.log(Level.WARN, e.getMessage(), e);
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        if (opened) {
            writer.close();
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        writer.flush();
    }
}
