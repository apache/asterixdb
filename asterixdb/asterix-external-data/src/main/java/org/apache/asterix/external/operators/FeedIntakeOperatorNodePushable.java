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

import org.apache.asterix.active.ActiveManager;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.message.ActivePartitionMessage;
import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.dataset.adapter.FeedAdapter;
import org.apache.asterix.external.feed.dataflow.DistributeFeedFrameWriter;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.runtime.AdapterRuntimeManager;
import org.apache.asterix.external.feed.runtime.IngestionRuntime;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * The runtime for @see{FeedIntakeOperationDescriptor}.
 * Provides the core functionality to set up the artifacts for ingestion of a feed.
 * The artifacts are lazily activated when a feed receives a subscription request.
 */
public class FeedIntakeOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

    private final EntityId feedId;
    private final int partition;
    private final IHyracksTaskContext ctx;
    private final IAdapterFactory adapterFactory;
    private final FeedIntakeOperatorDescriptor opDesc;

    public FeedIntakeOperatorNodePushable(IHyracksTaskContext ctx, EntityId feedId, IAdapterFactory adapterFactory,
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
        ActiveManager feedManager = (ActiveManager) ((IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject()).getActiveManager();
        AdapterRuntimeManager adapterRuntimeManager = null;
        DistributeFeedFrameWriter frameDistributor = null;
        IngestionRuntime ingestionRuntime = null;
        boolean open = false;
        try {
            Thread.currentThread().setName("Intake Thread");
            // create the adapter
            FeedAdapter adapter = (FeedAdapter) adapterFactory.createAdapter(ctx, partition);
            // create the distributor
            frameDistributor = new DistributeFeedFrameWriter(feedId, writer, FeedRuntimeType.INTAKE, partition);
            // create adapter runtime manager
            adapterRuntimeManager = new AdapterRuntimeManager(feedId, adapter, frameDistributor, partition);
            // create and register the runtime
            ActiveRuntimeId runtimeId = new ActiveRuntimeId(feedId, FeedRuntimeType.INTAKE.toString(), partition);
            ingestionRuntime = new IngestionRuntime(feedId, runtimeId, frameDistributor, adapterRuntimeManager, ctx);
            feedManager.registerRuntime(ingestionRuntime);
            // Notify FeedJobNotificationHandler that this provider is ready to receive subscription requests.
            ctx.sendApplicationMessageToCC(new ActivePartitionMessage(runtimeId, ctx.getJobletContext().getJobId(),
                    ActivePartitionMessage.ACTIVE_RUNTIME_REGISTERED), null);
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
            feedManager.deregisterRuntime(ingestionRuntime.getRuntimeId());
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
                feedManager.deregisterRuntime(ingestionRuntime.getRuntimeId());
            }
            throw new HyracksDataException(ie);
        } finally {
            if (open) {
                frameDistributor.close();
            }
        }
    }
}
