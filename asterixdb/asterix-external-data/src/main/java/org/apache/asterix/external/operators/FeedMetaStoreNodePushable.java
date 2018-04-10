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
import org.apache.asterix.common.dataflow.LSMInsertDeleteOperatorNodePushable;
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
import org.apache.hyracks.util.trace.ITracer;
import org.apache.hyracks.util.trace.TraceUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FeedMetaStoreNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private static final Logger LOGGER = LogManager.getLogger();

    /** Runtime node pushable corresponding to the core feed operator **/
    private AbstractUnaryInputUnaryOutputOperatorNodePushable insertOperator;

    /**
     * A policy accessor that ensures dyanmic decisions for a feed are taken
     * in accordance with the associated ingestion policy
     **/
    private final FeedPolicyAccessor policyAccessor;

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
    private final ActiveManager feedManager;

    private FrameTupleAccessor fta;

    private final IHyracksTaskContext ctx;

    private final VSizeFrame message;

    private final IRecordDescriptorProvider recordDescProvider;

    private final FeedMetaOperatorDescriptor opDesc;

    private final ITracer tracer;

    private final long traceCategory;

    public FeedMetaStoreNodePushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
            int partition, int nPartitions, IOperatorDescriptor coreOperator, FeedConnectionId feedConnectionId,
            Map<String, String> feedPolicyProperties, FeedMetaOperatorDescriptor feedMetaOperatorDescriptor)
            throws HyracksDataException {
        this.ctx = ctx;
        this.insertOperator = (AbstractUnaryInputUnaryOutputOperatorNodePushable) ((IActivity) coreOperator)
                .createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
        this.policyAccessor = new FeedPolicyAccessor(feedPolicyProperties);
        this.partition = partition;
        this.connectionId = feedConnectionId;
        this.feedManager = (ActiveManager) ((INcApplicationContext) ctx.getJobletContext().getServiceContext()
                .getApplicationContext()).getActiveManager();
        this.message = new VSizeFrame(ctx);
        TaskUtil.put(HyracksConstants.KEY_MESSAGE, message, ctx);
        this.recordDescProvider = recordDescProvider;
        this.opDesc = feedMetaOperatorDescriptor;
        tracer = ctx.getJobletContext().getServiceContext().getTracer();
        traceCategory = tracer.getRegistry().get(TraceUtils.STORAGE);
    }

    @Override
    public void open() throws HyracksDataException {
        ActiveRuntimeId runtimeId = new ActiveRuntimeId(connectionId.getFeedId(),
                runtimeType.toString() + "." + connectionId.getDatasetName(), partition);
        try {
            initializeNewFeedRuntime(runtimeId);
            insertOperator.open();
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Failed to open feed store operator", e);
            throw HyracksDataException.create(e);
        }
    }

    private void initializeNewFeedRuntime(ActiveRuntimeId runtimeId) throws Exception {
        fta = new FrameTupleAccessor(recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0));
        insertOperator.setOutputFrameWriter(0, writer, recordDesc);
        if (insertOperator instanceof LSMInsertDeleteOperatorNodePushable) {
            LSMInsertDeleteOperatorNodePushable indexOp = (LSMInsertDeleteOperatorNodePushable) insertOperator;
            if (!indexOp.isPrimary()) {
                writer = insertOperator;
                return;
            }
        }
        if (policyAccessor.flowControlEnabled()) {
            writer = new FeedRuntimeInputHandler(ctx, connectionId, runtimeId, insertOperator, policyAccessor, fta,
                    feedManager.getFramePool());
        } else {
            writer = new SyncFeedRuntimeInputHandler(ctx, insertOperator, fta);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        long tid = tracer.durationB("Ingestion-Store", traceCategory, null);
        try {
            FeedUtils.processFeedMessage(buffer, message, fta);
            writer.nextFrame(buffer);
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Failure Processing a frame at store side", e);
            throw HyracksDataException.create(e);
        } finally {
            tracer.durationE(tid, traceCategory, null);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        writer.close();
    }

    @Override
    public void flush() throws HyracksDataException {
        writer.flush();
    }

}
