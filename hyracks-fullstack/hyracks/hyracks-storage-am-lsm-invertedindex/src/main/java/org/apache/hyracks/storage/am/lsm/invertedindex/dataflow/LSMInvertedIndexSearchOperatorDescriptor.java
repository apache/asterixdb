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
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifierFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.common.IStorageManagerInterface;
import org.apache.hyracks.storage.common.file.NoOpLocalResourceFactoryProvider;

public class LSMInvertedIndexSearchOperatorDescriptor extends AbstractLSMInvertedIndexOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final int queryField;
    private final IInvertedIndexSearchModifierFactory searchModifierFactory;
    private final int[] minFilterFieldIndexes;
    private final int[] maxFilterFieldIndexes;

    public LSMInvertedIndexSearchOperatorDescriptor(IOperatorDescriptorRegistry spec, int queryField,
            IStorageManagerInterface storageManager, IFileSplitProvider fileSplitProvider,
            IIndexLifecycleManagerProvider lifecycleManagerProvider, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenComparatorFactories, ITypeTraits[] invListsTypeTraits,
            IBinaryComparatorFactory[] invListComparatorFactories,
            IIndexDataflowHelperFactory btreeDataflowHelperFactory, IBinaryTokenizerFactory queryTokenizerFactory,
            IInvertedIndexSearchModifierFactory searchModifierFactory, RecordDescriptor recDesc, boolean retainInput,
            boolean retainNull, INullWriterFactory nullWriterFactory,
            ISearchOperationCallbackFactory searchOpCallbackProvider, int[] minFilterFieldIndexes,
            int[] maxFilterFieldIndexes) {

        super(spec, 1, 1, recDesc, storageManager, fileSplitProvider, lifecycleManagerProvider, tokenTypeTraits,
                tokenComparatorFactories, invListsTypeTraits, invListComparatorFactories, queryTokenizerFactory,
                btreeDataflowHelperFactory, null, retainInput, retainNull, nullWriterFactory,
                NoOpLocalResourceFactoryProvider.INSTANCE, searchOpCallbackProvider,
                NoOpOperationCallbackFactory.INSTANCE);
        this.queryField = queryField;
        this.searchModifierFactory = searchModifierFactory;
        this.minFilterFieldIndexes = minFilterFieldIndexes;
        this.maxFilterFieldIndexes = maxFilterFieldIndexes;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        IInvertedIndexSearchModifier searchModifier = searchModifierFactory.createSearchModifier();
        return new LSMInvertedIndexSearchOperatorNodePushable(this, ctx, partition, recordDescProvider, queryField,
                searchModifier, minFilterFieldIndexes, maxFilterFieldIndexes);
    }
}
