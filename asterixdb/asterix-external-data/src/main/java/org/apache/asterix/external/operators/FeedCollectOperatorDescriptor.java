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

import org.apache.asterix.active.ActiveManager;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.external.feed.api.ISubscribableRuntime;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

/**
 * FeedCollectOperatorDescriptor is responsible for ingesting data from an external source. This
 * operator uses a user specified for a built-in adaptor for retrieving data from the external
 * data source.
 */
public class FeedCollectOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    /** The type associated with the ADM data output from (the feed adapter OR the compute operator) */
    private final IAType outputType;

    /** unique identifier for a feed instance. */
    private final FeedConnectionId connectionId;

    /** Map representation of policy parameters */
    private final Map<String, String> feedPolicyProperties;

    /** The source feed from which the feed derives its data from. **/
    private final EntityId sourceFeedId;

    /** The subscription location at which the recipient feed receives tuples from the source feed {SOURCE_FEED_INTAKE_STAGE , SOURCE_FEED_COMPUTE_STAGE} **/
    private final FeedRuntimeType subscriptionLocation;

    public FeedCollectOperatorDescriptor(JobSpecification spec, FeedConnectionId feedConnectionId,
            EntityId sourceFeedId, ARecordType atype, RecordDescriptor rDesc, Map<String, String> feedPolicyProperties,
            FeedRuntimeType subscriptionLocation) {
        super(spec, 0, 1);
        this.recordDescriptors[0] = rDesc;
        this.outputType = atype;
        this.connectionId = feedConnectionId;
        this.feedPolicyProperties = feedPolicyProperties;
        this.sourceFeedId = sourceFeedId;
        this.subscriptionLocation = subscriptionLocation;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        ActiveManager feedManager = (ActiveManager) ((IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject()).getActiveManager();
        ActiveRuntimeId sourceRuntimeId = new ActiveRuntimeId(sourceFeedId, subscriptionLocation.toString(), partition);
        ISubscribableRuntime sourceRuntime = (ISubscribableRuntime) feedManager.getRuntime(sourceRuntimeId);
        return new FeedCollectOperatorNodePushable(ctx, connectionId, feedPolicyProperties, partition, sourceRuntime);
    }

    public FeedConnectionId getFeedConnectionId() {
        return connectionId;
    }

    public Map<String, String> getFeedPolicyProperties() {
        return feedPolicyProperties;
    }

    public IAType getOutputType() {
        return outputType;
    }

    public RecordDescriptor getRecordDescriptor() {
        return recordDescriptors[0];
    }

    public EntityId getSourceFeedId() {
        return sourceFeedId;
    }

    public FeedRuntimeType getSubscriptionLocation() {
        return subscriptionLocation;
    }
}
