/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.metadata.feeds;

import java.util.Map;

import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

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
    private IOperatorDescriptor coreOperator;

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

    private final String operandId;

    public FeedMetaOperatorDescriptor(JobSpecification spec, FeedConnectionId feedConnectionId,
            IOperatorDescriptor coreOperatorDescriptor, Map<String, String> feedPolicyProperties,
            FeedRuntimeType runtimeType, boolean enableSubscriptionMode, String operandId) {
        super(spec, coreOperatorDescriptor.getInputArity(), coreOperatorDescriptor.getOutputArity());
        this.feedConnectionId = feedConnectionId;
        this.feedPolicyProperties = feedPolicyProperties;
        if (coreOperatorDescriptor.getOutputRecordDescriptors().length == 1) {
            recordDescriptors[0] = coreOperatorDescriptor.getOutputRecordDescriptors()[0];
        }
        this.coreOperator = coreOperatorDescriptor;
        this.runtimeType = runtimeType;
        this.operandId = operandId;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        IOperatorNodePushable nodePushable = null;
        switch (runtimeType) {
            case COMPUTE:
                nodePushable = new FeedMetaComputeNodePushable(ctx, recordDescProvider, partition, nPartitions,
                        coreOperator, feedConnectionId, feedPolicyProperties, operandId);
                break;
            case STORE:
                nodePushable = new FeedMetaStoreNodePushable(ctx, recordDescProvider, partition, nPartitions,
                        coreOperator, feedConnectionId, feedPolicyProperties, operandId);
                break;
            case OTHER:
                nodePushable = new FeedMetaNodePushable(ctx, recordDescProvider, partition, nPartitions, coreOperator,
                        feedConnectionId, feedPolicyProperties, operandId);
                break;
            case ETS:
                nodePushable = ((AlgebricksMetaOperatorDescriptor) coreOperator).createPushRuntime(ctx,
                        recordDescProvider, partition, nPartitions);
                break;
            case JOIN:
                break;
            default:
                throw new HyracksDataException(new IllegalArgumentException("Invalid feed runtime: " + runtimeType));
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
