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

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITupleFilterFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceFactoryProvider;

public abstract class AbstractLSMInvertedIndexOperatorDescriptor extends AbstractIndexOperatorDescriptor implements
        IInvertedIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    protected final ITypeTraits[] invListsTypeTraits;
    protected final IBinaryComparatorFactory[] invListComparatorFactories;
    protected final ITypeTraits[] tokenTypeTraits;
    protected final IBinaryComparatorFactory[] tokenComparatorFactories;
    protected final IBinaryTokenizerFactory tokenizerFactory;

    public AbstractLSMInvertedIndexOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputArity,
            int outputArity, RecordDescriptor recDesc, IStorageManagerInterface storageManager,
            IFileSplitProvider fileSplitProvider, IIndexLifecycleManagerProvider lifecycleManagerProvider,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenComparatorFactories,
            ITypeTraits[] invListsTypeTraits, IBinaryComparatorFactory[] invListComparatorFactories,
            IBinaryTokenizerFactory tokenizerFactory, IIndexDataflowHelperFactory dataflowHelperFactory,
            ITupleFilterFactory tupleFilterFactory, boolean retainInput,
            ILocalResourceFactoryProvider localResourceFactoryProvider,
            ISearchOperationCallbackFactory searchOpCallbackFactory,
            IModificationOperationCallbackFactory modificationOpCallbackFactory) {
        super(spec, inputArity, outputArity, recDesc, storageManager, lifecycleManagerProvider, fileSplitProvider,
                dataflowHelperFactory, tupleFilterFactory, retainInput, localResourceFactoryProvider,
                searchOpCallbackFactory, modificationOpCallbackFactory);
        this.invListsTypeTraits = invListsTypeTraits;
        this.invListComparatorFactories = invListComparatorFactories;
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenComparatorFactories = tokenComparatorFactories;
        this.tokenizerFactory = tokenizerFactory;
    }

    @Override
    public IBinaryComparatorFactory[] getTokenComparatorFactories() {
        return tokenComparatorFactories;
    }

    @Override
    public ITypeTraits[] getTokenTypeTraits() {
        return tokenTypeTraits;
    }

    @Override
    public IBinaryComparatorFactory[] getInvListsComparatorFactories() {
        return invListComparatorFactories;
    }

    @Override
    public IBinaryTokenizerFactory getTokenizerFactory() {
        return tokenizerFactory;
    }

    @Override
    public ITypeTraits[] getInvListsTypeTraits() {
        return invListsTypeTraits;
    }
}