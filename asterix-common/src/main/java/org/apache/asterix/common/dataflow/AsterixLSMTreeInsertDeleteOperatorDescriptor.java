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
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexInsertUpdateDeleteOperatorDescriptor;
import org.apache.hyracks.storage.common.IStorageManagerInterface;

public class AsterixLSMTreeInsertDeleteOperatorDescriptor extends LSMTreeIndexInsertUpdateDeleteOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final boolean isPrimary;

    /** the name of the index that is being operated upon **/
    private final String indexName;

    public AsterixLSMTreeInsertDeleteOperatorDescriptor(IOperatorDescriptorRegistry spec,
            RecordDescriptor recDesc, IStorageManagerInterface storageManager,
            IIndexLifecycleManagerProvider lifecycleManagerProvider, IFileSplitProvider fileSplitProvider,
            ITypeTraits[] typeTraits, IBinaryComparatorFactory[] comparatorFactories, int[] bloomFilterKeyFields,
            int[] fieldPermutation, IndexOperation op, IIndexDataflowHelperFactory dataflowHelperFactory,
            ITupleFilterFactory tupleFilterFactory,
            IModificationOperationCallbackFactory modificationOpCallbackProvider, boolean isPrimary, String indexName) {
        super(spec, recDesc, storageManager, lifecycleManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, bloomFilterKeyFields, fieldPermutation, op, dataflowHelperFactory,
                tupleFilterFactory, modificationOpCallbackProvider);
        this.isPrimary = isPrimary;
        this.indexName = indexName;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new AsterixLSMInsertDeleteOperatorNodePushable(this, ctx, partition, fieldPermutation,
                recordDescProvider, op, isPrimary);
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public String getIndexName() {
        return indexName;
    }

    public int[] getFieldPermutations() {
        return fieldPermutation;
    }

    public IndexOperation getIndexOperation() {
        return op;
    }

    public IBinaryComparatorFactory[] getComparatorFactories() {
        return comparatorFactories;
    }
}
