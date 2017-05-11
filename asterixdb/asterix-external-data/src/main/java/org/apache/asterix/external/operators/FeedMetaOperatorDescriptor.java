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

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

/**
 * FeedMetaOperatorDescriptor is a wrapper operator that provides a sanboox like
 * environment for an hyracks operator that is part of a feed ingestion
 * pipeline. The MetaFeed operator provides an interface iden- tical to that
 * offered by the underlying wrapped operator, hereafter referred to as the core
 * operator. As seen by Hyracks, the altered pipeline is identical to the
 * earlier version formed from core operators. The MetaFeed operator enhances
 * each core operator by providing functionality for handling runtime
 * exceptions, saving any state for future retrieval, and measuring/reporting of
 * performance characteristics. We next describe how the added functionality
 * contributes to providing fault- tolerance.
 */

public class FeedMetaOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    /**
     * The actual (Hyracks) operator that is wrapped around by the MetaFeed
     * operator.
     **/
    private final IOperatorDescriptor coreOperator;

    /**
     * A unique identifier for the feed instance. A feed instance represents the
     * flow of data from a feed to a dataset.
     **/
    private final FeedConnectionId feedConnectionId;

    /**
     * The policy associated with the feed instance.
     **/
    private final Map<String, String> feedPolicyProperties;

    /**
     * type for the feed runtime associated with the operator.
     * Possible values: COMPUTE, STORE, OTHER
     **/
    private final FeedRuntimeType runtimeType;

    public FeedMetaOperatorDescriptor(final JobSpecification spec, final FeedConnectionId feedConnectionId,
            final IOperatorDescriptor coreOperatorDescriptor, final Map<String, String> feedPolicyProperties,
            final FeedRuntimeType runtimeType) {
        super(spec, coreOperatorDescriptor.getInputArity(), coreOperatorDescriptor.getOutputArity());
        this.feedConnectionId = feedConnectionId;
        this.feedPolicyProperties = feedPolicyProperties;
        if (coreOperatorDescriptor.getOutputRecordDescriptors().length == 1) {
            outRecDescs[0] = coreOperatorDescriptor.getOutputRecordDescriptors()[0];
        }
        this.coreOperator = coreOperatorDescriptor;
        this.runtimeType = runtimeType;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
            throws HyracksDataException {
        IOperatorNodePushable nodePushable = null;
        switch (runtimeType) {
            case COMPUTE:
                nodePushable = new FeedMetaComputeNodePushable(ctx, recordDescProvider, partition, nPartitions,
                        coreOperator, feedConnectionId, feedPolicyProperties, this);
                break;
            case STORE:
                nodePushable = new FeedMetaStoreNodePushable(ctx, recordDescProvider, partition, nPartitions,
                        coreOperator, feedConnectionId, feedPolicyProperties, this);
                break;
            default:
                throw new RuntimeDataException(ErrorCode.OPERATORS_FEED_META_OPERATOR_DESCRIPTOR_INVALID_RUNTIME,
                        runtimeType);
        }
        return nodePushable;
    }

    @Override
    public String toString() {
        return "FeedMeta [" + coreOperator + " ]";
    }

    public IOperatorDescriptor getCoreOperator() {
        return coreOperator;
    }

    public FeedRuntimeType getRuntimeType() {
        return runtimeType;
    }

}
