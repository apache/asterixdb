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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.dataflow.LSMIndexCompactOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.file.NoOpLocalResourceFactoryProvider;

public class LSMInvertedIndexCompactOperator extends AbstractLSMInvertedIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    public LSMInvertedIndexCompactOperator(IOperatorDescriptorRegistry spec, IStorageManagerInterface storageManager,
            IFileSplitProvider fileSplitProvider, IIndexLifecycleManagerProvider lifecycleManagerProvider,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenComparatorFactories,
            ITypeTraits[] invListsTypeTraits, IBinaryComparatorFactory[] invListComparatorFactories,
            IBinaryTokenizerFactory tokenizerFactory, IIndexDataflowHelperFactory dataflowHelperFactory,
            IModificationOperationCallbackFactory modificationOpCallbackFactory) {
        super(spec, 1, 1, null, storageManager, fileSplitProvider, lifecycleManagerProvider, tokenTypeTraits,
                tokenComparatorFactories, invListsTypeTraits, invListComparatorFactories, tokenizerFactory,
                dataflowHelperFactory, null, false, NoOpLocalResourceFactoryProvider.INSTANCE,
                NoOpOperationCallbackFactory.INSTANCE, modificationOpCallbackFactory);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new LSMIndexCompactOperatorNodePushable(this, ctx, partition);
    }
}