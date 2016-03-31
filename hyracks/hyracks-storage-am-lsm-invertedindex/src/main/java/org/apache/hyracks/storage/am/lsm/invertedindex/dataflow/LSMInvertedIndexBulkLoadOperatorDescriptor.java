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

package org.apache.hyracks.storage.am.lsm.invertedindex.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexBulkLoadOperatorNodePushable;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.common.IStorageManagerInterface;
import org.apache.hyracks.storage.common.file.NoOpLocalResourceFactoryProvider;

public class LSMInvertedIndexBulkLoadOperatorDescriptor extends AbstractLSMInvertedIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final int[] fieldPermutation;
    private final boolean verifyInput;
    private final long numElementsHint;
    private final boolean checkIfEmptyIndex;

    public LSMInvertedIndexBulkLoadOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recDesc,
            int[] fieldPermutation, boolean verifyInput, long numElementsHint, boolean checkIfEmptyIndex,
            IStorageManagerInterface storageManager, IFileSplitProvider fileSplitProvider,
            IIndexLifecycleManagerProvider lifecycleManagerProvider, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenComparatorFactories, ITypeTraits[] invListsTypeTraits,
            IBinaryComparatorFactory[] invListComparatorFactories, IBinaryTokenizerFactory tokenizerFactory,
            IIndexDataflowHelperFactory invertedIndexDataflowHelperFactory) {
        super(spec, 1, 1, recDesc, storageManager, fileSplitProvider, lifecycleManagerProvider, tokenTypeTraits,
                tokenComparatorFactories, invListsTypeTraits, invListComparatorFactories, tokenizerFactory,
                invertedIndexDataflowHelperFactory, null, false, false, null,
                NoOpLocalResourceFactoryProvider.INSTANCE, NoOpOperationCallbackFactory.INSTANCE,
                NoOpOperationCallbackFactory.INSTANCE);
        this.fieldPermutation = fieldPermutation;
        this.verifyInput = verifyInput;
        this.numElementsHint = numElementsHint;
        this.checkIfEmptyIndex = checkIfEmptyIndex;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new IndexBulkLoadOperatorNodePushable(this, ctx, partition, fieldPermutation, 1.0f, verifyInput,
                numElementsHint, checkIfEmptyIndex, recordDescProvider);
    }
}
