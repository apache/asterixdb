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
package edu.uci.ics.hyracks.dataflow.std.group.preclustered;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class PreclusteredGroupOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private final int[] groupFields;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IAggregatorDescriptorFactory aggregatorFactory;

    private static final long serialVersionUID = 1L;

    public PreclusteredGroupOperatorDescriptor(IOperatorDescriptorRegistry spec, int[] groupFields,
            IBinaryComparatorFactory[] comparatorFactories, IAggregatorDescriptorFactory aggregatorFactory,
            RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.groupFields = groupFields;
        this.comparatorFactories = comparatorFactories;
        this.aggregatorFactory = aggregatorFactory;
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
            throws HyracksDataException {
        return new PreclusteredGroupOperatorNodePushable(ctx, groupFields, comparatorFactories, aggregatorFactory,
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0), recordDescriptors[0]);
    }
}