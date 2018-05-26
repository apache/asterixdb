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
package org.apache.asterix.common.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexInsertUpdateDeleteOperatorDescriptor;

public class LSMTreeInsertDeleteOperatorDescriptor extends LSMTreeIndexInsertUpdateDeleteOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final boolean isPrimary;

    public LSMTreeInsertDeleteOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc,
            int[] fieldPermutation, IndexOperation op, IIndexDataflowHelperFactory indexHelperFactory,
            ITupleFilterFactory tupleFilterFactory, boolean isPrimary,
            IModificationOperationCallbackFactory modCallbackFactory) {
        super(spec, outRecDesc, indexHelperFactory, fieldPermutation, op, modCallbackFactory, tupleFilterFactory);
        this.isPrimary = isPrimary;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new LSMInsertDeleteOperatorNodePushable(ctx, partition, fieldPermutation, inputRecDesc, op, isPrimary,
                indexHelperFactory, modCallbackFactory, tupleFilterFactory, sourceLoc);
    }

    public boolean isPrimary() {
        return isPrimary;
    }
}
