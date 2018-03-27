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
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

/**
 * The first operator in a collect job in a feed.
 */
public class FeedCollectOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private final int partition;
    private final FeedConnectionId connectionId;
    private final FeedPolicyAccessor policyAccessor;
    private final ActiveManager activeManager;
    private final IHyracksTaskContext ctx;

    public FeedCollectOperatorNodePushable(IHyracksTaskContext ctx, FeedConnectionId feedConnectionId,
            Map<String, String> feedPolicy, int partition) {
        this.ctx = ctx;
        this.partition = partition;
        this.connectionId = feedConnectionId;
        this.policyAccessor = new FeedPolicyAccessor(feedPolicy);
        this.activeManager = (ActiveManager) ((INcApplicationContext) ctx.getJobletContext().getServiceContext()
                .getApplicationContext()).getActiveManager();
    }

    @Override
    public void initialize() throws HyracksDataException {
        try {
            ActiveRuntimeId runtimeId =
                    new ActiveRuntimeId(connectionId.getFeedId(), FeedRuntimeType.COLLECT.toString(), partition);
            FrameTupleAccessor tAccessor = new FrameTupleAccessor(recordDesc);
            if (policyAccessor.flowControlEnabled()) {
                writer = new FeedRuntimeInputHandler(ctx, connectionId, runtimeId, writer, policyAccessor, tAccessor,
                        activeManager.getFramePool());
            } else {
                writer = new SyncFeedRuntimeInputHandler(ctx, writer, tAccessor);
            }
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        writer.nextFrame(buffer);
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void flush() throws HyracksDataException {
        writer.flush();
    }

    @Override
    public void close() throws HyracksDataException {
        writer.close();
    }
}
