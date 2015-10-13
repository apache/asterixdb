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

import java.util.logging.Logger;

import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.active.ActiveObjectId.ActiveObjectType;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

/**
 * A repetitive channel operator, which uses a Java timer to run a given query periodically
 */
public class RepetitiveChannelOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(RepetitiveChannelOperatorDescriptor.class.getName());

    /** The unique identifier of the channel. **/
    private final ActiveJobId channelJobId;

    private final FunctionSignature function;

    private final String duration;

    private final String subscriptionsName;

    private final String resultsName;

    public RepetitiveChannelOperatorDescriptor(JobSpecification spec, String dataverseName, String channelName,
            String duration, FunctionSignature function, String subscriptionsName, String resultsName) {
        super(spec, 0, 1);
        this.channelJobId = new ActiveJobId(dataverseName, channelName, ActiveObjectType.CHANNEL);
        this.duration = duration;
        this.function = function;
        this.subscriptionsName = subscriptionsName;
        this.resultsName = resultsName;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new RepetitiveChannelOperatorNodePushable(ctx, channelJobId, function, duration, subscriptionsName,
                resultsName);
    }

    public ActiveJobId getChannelJobId() {
        return channelJobId;
    }

    public String getDuration() {
        return duration;
    }

    public String getSubscriptionsName() {
        return subscriptionsName;
    }

    public String getResultsName() {
        return resultsName;
    }

    public FunctionSignature getFunction() {
        return function;
    }

}
