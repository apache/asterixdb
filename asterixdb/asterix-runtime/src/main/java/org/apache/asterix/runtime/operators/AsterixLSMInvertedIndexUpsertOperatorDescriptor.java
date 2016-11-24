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

import org.apache.asterix.common.dataflow.AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.common.IStorageManagerInterface;

public class AsterixLSMInvertedIndexUpsertOperatorDescriptor
        extends AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final int[] prevFieldPermutation;

    public AsterixLSMInvertedIndexUpsertOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recDesc,
            IStorageManagerInterface storageManager, IFileSplitProvider fileSplitProvider,
            IIndexLifecycleManagerProvider lifecycleManagerProvider, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenComparatorFactories, ITypeTraits[] invListsTypeTraits,
            IBinaryComparatorFactory[] invListComparatorFactories, IBinaryTokenizerFactory tokenizerFactory,
            int[] fieldPermutation, IIndexDataflowHelperFactory dataflowHelperFactory,
            ITupleFilterFactory tupleFilterFactory, IModificationOperationCallbackFactory modificationOpCallbackFactory,
            String indexName, int[] prevFieldPermutation) {
        super(spec, recDesc, storageManager, fileSplitProvider, lifecycleManagerProvider, tokenTypeTraits,
                tokenComparatorFactories, invListsTypeTraits, invListComparatorFactories, tokenizerFactory,
                fieldPermutation, IndexOperation.UPSERT, dataflowHelperFactory, tupleFilterFactory,
                modificationOpCallbackFactory, indexName);
        this.prevFieldPermutation = prevFieldPermutation;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new AsterixLSMSecondaryUpsertOperatorNodePushable(this, ctx, partition, fieldPermutation,
                recordDescProvider, prevFieldPermutation);
    }
}
