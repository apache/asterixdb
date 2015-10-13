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
package org.apache.asterix.metadata.channels;

import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.feeds.api.IActiveRuntime.ActiveRuntimeType;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

/**
 * ChannelMetaOperatorDescriptor is a wrapper operator that provides a sanboox like
 * environment for an hyracks operator that is part of a channel
 * The MetaChannel operator provides an interface identical to that
 * offered by the underlying wrapped operator, hereafter referred to as the core
 * operator. As seen by Hyracks, the altered pipeline is identical to the
 * earlier version formed from core operators. The MetaChannel operator enhances
 * each core operator by providing functionality for handling runtime
 * exceptions, saving any state for future retrieval, and measuring/reporting of
 * performance characteristics. We next describe how the added functionality
 * contributes to providing fault- tolerance.
 */

public class ChannelMetaOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    /**
     * The actual (Hyracks) operator that is wrapped around by the MetaFeed
     * operator.
     **/
    private IOperatorDescriptor coreOperator;

    /**
     * A unique identifier for the channel instance.
     **/
    private final ActiveJobId channelJobId;

    /**
     * type for the feed runtime associated with the operator.
     * Possible values: REPETITIVE, CONTINUOUS
     **/
    private final ActiveRuntimeType runtimeType;

    private final FunctionSignature function;

    private final String duration;

    private final String subscriptionsName;

    private final String resultsName;

    public ChannelMetaOperatorDescriptor(JobSpecification spec, ActiveJobId channelJobId,
            IOperatorDescriptor coreOperatorDescriptor, ActiveRuntimeType runtimeType, FunctionSignature function,
            String duration, String subscriptionsName, String resultsName) {
        super(spec, coreOperatorDescriptor.getInputArity(), coreOperatorDescriptor.getOutputArity());
        this.channelJobId = channelJobId;
        if (coreOperatorDescriptor.getOutputRecordDescriptors().length == 1) {
            recordDescriptors[0] = coreOperatorDescriptor.getOutputRecordDescriptors()[0];
        }
        this.coreOperator = coreOperatorDescriptor;
        this.runtimeType = runtimeType;
        this.function = function;
        this.duration = duration;
        this.subscriptionsName = subscriptionsName;
        this.resultsName = resultsName;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        IOperatorNodePushable nodePushable = null;
        switch (runtimeType) {
            case REPETITIVE:
                nodePushable = new RepetitiveChannelOperatorNodePushable(ctx, channelJobId, function, duration,
                        subscriptionsName, resultsName);
                break;
            default:
                throw new HyracksDataException(new IllegalArgumentException("Invalid channel runtime: " + runtimeType));
        }
        return nodePushable;
    }

    @Override
    public String toString() {
        return "ChannelMeta [" + coreOperator + " ]";
    }

    public IOperatorDescriptor getCoreOperator() {
        return coreOperator;
    }

    public ActiveRuntimeType getRuntimeType() {
        return runtimeType;
    }

}
