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

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.ActiveSourceOperatorNodePushable;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.dataset.adapter.FeedAdapter;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.runtime.AdapterRuntimeManager;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;

/**
 * The runtime for @see{FeedIntakeOperationDescriptor}.
 * Provides the core functionality to set up the artifacts for ingestion of a feed.
 * The artifacts are lazily activated when a feed receives a subscription request.
 */
public class FeedIntakeOperatorNodePushable extends ActiveSourceOperatorNodePushable {

    private final int partition;
    private final IAdapterFactory adapterFactory;
    private final FeedIntakeOperatorDescriptor opDesc;
    private volatile AdapterRuntimeManager adapterRuntimeManager;

    public FeedIntakeOperatorNodePushable(IHyracksTaskContext ctx, EntityId feedId, IAdapterFactory adapterFactory,
            int partition, FeedPolicyAccessor policyAccessor, IRecordDescriptorProvider recordDescProvider,
            FeedIntakeOperatorDescriptor feedIntakeOperatorDescriptor) {
        super(ctx, new ActiveRuntimeId(feedId, FeedIntakeOperatorNodePushable.class.getSimpleName(), partition));
        this.opDesc = feedIntakeOperatorDescriptor;
        this.recordDesc = recordDescProvider.getOutputRecordDescriptor(opDesc.getActivityId(), 0);
        this.partition = partition;
        this.adapterFactory = adapterFactory;
    }

    @Override
    protected void start() throws HyracksDataException, InterruptedException {
        writer.open();
        try {
            Thread.currentThread().setName("Intake Thread");
            FeedAdapter adapter = (FeedAdapter) adapterFactory.createAdapter(ctx, partition);
            adapterRuntimeManager = new AdapterRuntimeManager(ctx, runtimeId.getEntityId(), adapter, writer, partition);
            TaskUtil.putInSharedMap(HyracksConstants.KEY_MESSAGE, new VSizeFrame(ctx), ctx);
            adapterRuntimeManager.start();
            synchronized (adapterRuntimeManager) {
                while (!adapterRuntimeManager.isDone()) {
                    adapterRuntimeManager.wait();
                }
            }
            if (adapterRuntimeManager.isFailed()) {
                throw new RuntimeDataException(
                        ErrorCode.OPERATORS_FEED_INTAKE_OPERATOR_NODE_PUSHABLE_FAIL_AT_INGESTION);
            }
        } catch (Exception e) {
            /*
             * An Interrupted Exception is thrown if the Intake job cannot progress further due to failure of another
             * node involved in the Hyracks job. As the Intake job involves only the intake operator, the exception is
             * indicative of a failure at the sibling intake operator location. The surviving intake partitions must
             * continue to live and receive data from the external source.
             */
            writer.fail();
            throw e;
        } finally {
            writer.close();
        }
    }

    @Override
    protected void abort() throws HyracksDataException, InterruptedException {
        if (adapterRuntimeManager != null) {
            adapterRuntimeManager.stop();
        }
    }
}
