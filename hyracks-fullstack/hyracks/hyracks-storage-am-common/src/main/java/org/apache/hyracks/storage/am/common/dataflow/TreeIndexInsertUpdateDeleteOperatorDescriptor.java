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

package org.apache.hyracks.storage.am.common.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;

public class TreeIndexInsertUpdateDeleteOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final IIndexDataflowHelperFactory indexHelperFactory;
    private final IndexOperation op;
    private final int[] fieldPermutation;
    private final IModificationOperationCallbackFactory modificationOpCallbackFactory;
    private final ITupleFilterFactory tupleFilterFactory;

    public TreeIndexInsertUpdateDeleteOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc,
            int[] fieldPermutation, IndexOperation op, IIndexDataflowHelperFactory indexHelperFactory,
            ITupleFilterFactory tupleFilterFactory,
            IModificationOperationCallbackFactory modificationOpCallbackFactory) {
        super(spec, 1, 1);
        this.indexHelperFactory = indexHelperFactory;
        this.fieldPermutation = fieldPermutation;
        this.op = op;
        this.modificationOpCallbackFactory = modificationOpCallbackFactory;
        this.tupleFilterFactory = tupleFilterFactory;
        this.outRecDescs[0] = outRecDesc;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new IndexInsertUpdateDeleteOperatorNodePushable(ctx, partition, indexHelperFactory, fieldPermutation,
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0), op, modificationOpCallbackFactory,
                tupleFilterFactory);
    }
}
