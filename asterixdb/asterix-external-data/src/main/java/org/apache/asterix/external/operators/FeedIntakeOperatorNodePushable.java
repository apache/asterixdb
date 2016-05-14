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

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.dataset.adapter.FeedAdapter;
import org.apache.asterix.external.feed.api.IFeedRuntime.FeedRuntimeType;
import org.apache.asterix.external.feed.dataflow.DistributeFeedFrameWriter;
import org.apache.asterix.external.feed.management.FeedId;
import org.apache.asterix.external.feed.management.FeedManager;
import org.apache.asterix.external.feed.message.FeedPartitionStartMessage;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.runtime.AdapterRuntimeManager;
import org.apache.asterix.external.feed.runtime.FeedRuntimeId;
import org.apache.asterix.external.feed.runtime.IngestionRuntime;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * The runtime for @see{FeedIntakeOperationDescriptor}.
 * Provides the core functionality to set up the artifacts for ingestion of a feed.
 * The artifacts are lazily activated when a feed receives a subscription request.
 */
public class FeedIntakeOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

    private final FeedId feedId;
    private final int partition;
    private final IHyracksTaskContext ctx;
    private final IAdapterFactory adapterFactory;
    private final FeedIntakeOperatorDescriptor opDesc;

    public FeedIntakeOperatorNodePushable(IHyracksTaskContext ctx, FeedId feedId, IAdapterFactory adapterFactory,
            int partition, FeedPolicyAccessor policyAccessor, IRecordDescriptorProvider recordDescProvider,
            FeedIntakeOperatorDescriptor feedIntakeOperatorDescriptor) {
        this.opDesc = feedIntakeOperatorDescriptor;
        this.recordDesc = recordDescProvider.getOutputRecordDescriptor(opDesc.getActivityId(), 0);
        this.ctx = ctx;
        this.feedId = feedId;
        this.partition = partition;
        this.adapterFactory = adapterFactory;
    }

    @Override
    public void initialize() throws HyracksDataException {
        FeedManager feedManager = (FeedManager) ((IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject()).getFeedManager();
        AdapterRuntimeManager adapterRuntimeManager = null;
        DistributeFeedFrameWriter frameDistributor = null;
        IngestionRuntime ingestionRuntime = null;
        boolean open = false;
        try {
            Thread.currentThread().setName("Intake Thread");
            // create the adapter
            FeedAdapter adapter = (FeedAdapter) adapterFactory.createAdapter(ctx, partition);
            // create the distributor
            frameDistributor = new DistributeFeedFrameWriter(ctx, feedId, writer, FeedRuntimeType.INTAKE, partition,
                    new FrameTupleAccessor(recordDesc));
            // create adapter runtime manager
            adapterRuntimeManager = new AdapterRuntimeManager(feedId, adapter, frameDistributor, partition);
            // create and register the runtime
            FeedRuntimeId runtimeId =
                    new FeedRuntimeId(feedId, FeedRuntimeType.INTAKE, partition, FeedRuntimeId.DEFAULT_TARGET_ID);
            ingestionRuntime = new IngestionRuntime(feedId, runtimeId, frameDistributor, adapterRuntimeManager, ctx);
            feedManager.registerFeedSubscribableRuntime(ingestionRuntime);
            // Notify FeedJobNotificationHandler that this provider is ready to receive subscription requests.
            ctx.sendApplicationMessageToCC(new FeedPartitionStartMessage(feedId, ctx.getJobletContext().getJobId()),
                    null);
            // open the distributor
            open = true;
            frameDistributor.open();
            // wait until ingestion is over
            synchronized (adapterRuntimeManager) {
                while (!adapterRuntimeManager.isDone()) {
                    adapterRuntimeManager.wait();
                }
            }
            // The ingestion is over. we need to remove the runtime from the manager
            feedManager.deregisterFeedSubscribableRuntime(ingestionRuntime.getRuntimeId());
            // If there was a failure, we need to throw an exception
            if (adapterRuntimeManager.isFailed()) {
                throw new HyracksDataException("Unable to ingest data");
            }
        } catch (Throwable ie) {
            /*
             * An Interrupted Exception is thrown if the Intake job cannot progress further due to failure of another node involved in the Hyracks job.
             * As the Intake job involves only the intake operator, the exception is indicative of a failure at the sibling intake operator location.
             * The surviving intake partitions must continue to live and receive data from the external source.
             */
            if (ingestionRuntime != null) {
                ingestionRuntime.terminate();
                feedManager.deregisterFeedSubscribableRuntime(ingestionRuntime.getRuntimeId());
            }
            throw new HyracksDataException(ie);
        } finally {
            if (open) {
                frameDistributor.close();
            }
        }
    }
}
