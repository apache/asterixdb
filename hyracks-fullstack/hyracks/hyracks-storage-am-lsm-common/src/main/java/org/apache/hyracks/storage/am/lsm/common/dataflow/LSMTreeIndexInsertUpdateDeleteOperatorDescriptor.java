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

package org.apache.hyracks.storage.am.lsm.common.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;

public class LSMTreeIndexInsertUpdateDeleteOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    protected final int[] fieldPermutation;
    protected final IndexOperation op;
    protected final IIndexDataflowHelperFactory indexHelperFactory;
    protected final IModificationOperationCallbackFactory modCallbackFactory;
    protected final ITupleFilterFactory tupleFilterFactory;

    public LSMTreeIndexInsertUpdateDeleteOperatorDescriptor(IOperatorDescriptorRegistry spec,
            RecordDescriptor outRecDesc, IIndexDataflowHelperFactory indexHelperFactory, int[] fieldPermutation,
            IndexOperation op, IModificationOperationCallbackFactory modCallbackFactory,
            ITupleFilterFactory tupleFilterFactory) {
        super(spec, 1, 1);
        this.indexHelperFactory = indexHelperFactory;
        this.modCallbackFactory = modCallbackFactory;
        this.tupleFilterFactory = tupleFilterFactory;
        this.fieldPermutation = fieldPermutation;
        this.op = op;
        this.outRecDescs[0] = outRecDesc;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new LSMIndexInsertUpdateDeleteOperatorNodePushable(ctx, partition, indexHelperFactory, fieldPermutation,
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0), op, modCallbackFactory,
                tupleFilterFactory);
    }
}
