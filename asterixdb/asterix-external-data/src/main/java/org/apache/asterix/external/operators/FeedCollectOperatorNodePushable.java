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

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.external.feed.api.IFeedRuntime.FeedRuntimeType;
import org.apache.asterix.external.feed.api.ISubscribableRuntime;
import org.apache.asterix.external.feed.dataflow.FeedFrameCollector;
import org.apache.asterix.external.feed.dataflow.FeedRuntimeInputHandler;
import org.apache.asterix.external.feed.dataflow.SyncFeedRuntimeInputHandler;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.management.FeedId;
import org.apache.asterix.external.feed.management.FeedManager;
import org.apache.asterix.external.feed.message.FeedPartitionStartMessage;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.runtime.CollectionRuntime;
import org.apache.asterix.external.feed.runtime.FeedRuntimeId;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * The first operator in a collect job in a feed.
 */
public class FeedCollectOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

    private final int partition;
    private final FeedConnectionId connectionId;
    private final Map<String, String> feedPolicy;
    private final FeedPolicyAccessor policyAccessor;
    private final FeedManager feedManager;
    private final ISubscribableRuntime sourceRuntime;
    private final IHyracksTaskContext ctx;
    private CollectionRuntime collectRuntime;

    public FeedCollectOperatorNodePushable(IHyracksTaskContext ctx, FeedId sourceFeedId,
            FeedConnectionId feedConnectionId, Map<String, String> feedPolicy, int partition, int nPartitions,
            ISubscribableRuntime sourceRuntime) {
        this.ctx = ctx;
        this.partition = partition;
        this.connectionId = feedConnectionId;
        this.sourceRuntime = sourceRuntime;
        this.feedPolicy = feedPolicy;
        this.policyAccessor = new FeedPolicyAccessor(feedPolicy);
        this.feedManager = (FeedManager) ((IAsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext()
                .getApplicationObject()).getFeedManager();
    }

    @Override
    public void initialize() throws HyracksDataException {
        try {
            FeedRuntimeId runtimeId = new FeedRuntimeId(connectionId.getFeedId(), FeedRuntimeType.COLLECT, partition,
                    FeedRuntimeId.DEFAULT_TARGET_ID);
            // Does this collector have a handler?
            FrameTupleAccessor tAccessor = new FrameTupleAccessor(recordDesc);
            if (policyAccessor.bufferingEnabled()) {
                writer = new FeedRuntimeInputHandler(ctx, connectionId, runtimeId, writer, policyAccessor, tAccessor,
                        feedManager.getFeedMemoryManager());
            } else {
                writer = new SyncFeedRuntimeInputHandler(ctx, writer, tAccessor);
            }
            collectRuntime = new CollectionRuntime(connectionId, runtimeId, sourceRuntime, feedPolicy, ctx,
                    new FeedFrameCollector(policyAccessor, writer, connectionId));
            feedManager.getFeedConnectionManager().registerFeedRuntime(connectionId, collectRuntime);
            sourceRuntime.subscribe(collectRuntime);
            // Notify CC that Collection started
            ctx.sendApplicationMessageToCC(
                    new FeedPartitionStartMessage(connectionId.getFeedId(), ctx.getJobletContext().getJobId()), null);
            collectRuntime.waitTillCollectionOver();
            feedManager.getFeedConnectionManager().deRegisterFeedRuntime(connectionId, collectRuntime.getRuntimeId());
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }
}
