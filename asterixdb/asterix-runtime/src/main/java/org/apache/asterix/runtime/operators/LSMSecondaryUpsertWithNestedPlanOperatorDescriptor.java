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
package org.apache.asterix.runtime.operators;

import java.util.List;

import org.apache.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;

public class LSMSecondaryUpsertWithNestedPlanOperatorDescriptor extends LSMSecondaryUpsertOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final List<AlgebricksPipeline> secondaryKeysPipeline;
    private final List<AlgebricksPipeline> prevSecondaryKeysPipeline;

    public LSMSecondaryUpsertWithNestedPlanOperatorDescriptor(JobSpecification spec, RecordDescriptor outRecDesc,
            int[] fieldPermutation, IIndexDataflowHelperFactory indexHelperFactory,
            IModificationOperationCallbackFactory modCallbackFactory, int operationFieldIndex,
            IBinaryIntegerInspectorFactory operationInspectorFactory, List<AlgebricksPipeline> secondaryKeysPipeline,
            List<AlgebricksPipeline> prevSecondaryKeysPipeline, ITuplePartitionerFactory tuplePartitionerFactory,
            int[][] partitionsMap) {
        super(spec, outRecDesc, fieldPermutation, indexHelperFactory, null, null, modCallbackFactory,
                operationFieldIndex, operationInspectorFactory, null, tuplePartitionerFactory, partitionsMap);
        this.secondaryKeysPipeline = secondaryKeysPipeline;
        this.prevSecondaryKeysPipeline = prevSecondaryKeysPipeline;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new LSMSecondaryUpsertWithNestedPlanOperatorNodePushable(ctx, partition, indexHelperFactory,
                modCallbackFactory, fieldPermutation, inputRecDesc, operationFieldIndex, operationInspectorFactory,
                secondaryKeysPipeline, prevSecondaryKeysPipeline, tuplePartitionerFactory, partitionsMap);
    }
}
