/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITupleFilterFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public abstract class AbstractLSMInvertedIndexOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor
        implements IInvertedIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    // General.
    protected final IStorageManagerInterface storageManager;
    protected final IIndexLifecycleManagerProvider lifecycleManagerProvider;
    protected final boolean retainInput;
    protected final IOperationCallbackProvider opCallbackProvider;

    // Btree.
    protected final ITypeTraits[] tokenTypeTraits;
    protected final IBinaryComparatorFactory[] tokenComparatorFactories;
    protected final IFileSplitProvider btreeFileSplitProvider;

    // Inverted index.
    protected final ITypeTraits[] invListsTypeTraits;
    protected final IBinaryComparatorFactory[] invListComparatorFactories;
    protected final IBinaryTokenizerFactory tokenizerFactory;
    protected final IFileSplitProvider invListsFileSplitProvider;
    protected final IIndexDataflowHelperFactory invertedIndexDataflowHelperFactory;

    public AbstractLSMInvertedIndexOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputArity, int outputArity,
            RecordDescriptor recDesc, IStorageManagerInterface storageManager,
            IFileSplitProvider btreeFileSplitProvider, IFileSplitProvider invListsFileSplitProvider,
            IIndexLifecycleManagerProvider lifecycleManagerProvider, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenComparatorFactories, ITypeTraits[] invListsTypeTraits,
            IBinaryComparatorFactory[] invListComparatorFactories, IBinaryTokenizerFactory tokenizerFactory,
            IIndexDataflowHelperFactory invertedIndexDataflowHelperFactory, boolean retainInput,
            IOperationCallbackProvider opCallbackProvider) {
        super(spec, inputArity, outputArity);

        // General.
        this.storageManager = storageManager;
        this.lifecycleManagerProvider = lifecycleManagerProvider;
        this.retainInput = retainInput;
        this.opCallbackProvider = opCallbackProvider;

        // Btree.
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenComparatorFactories = tokenComparatorFactories;
        this.btreeFileSplitProvider = btreeFileSplitProvider;

        // Inverted index.
        this.invListsTypeTraits = invListsTypeTraits;
        this.invListComparatorFactories = invListComparatorFactories;
        this.tokenizerFactory = tokenizerFactory;
        this.invListsFileSplitProvider = invListsFileSplitProvider;
        this.invertedIndexDataflowHelperFactory = invertedIndexDataflowHelperFactory;

        if (outputArity > 0) {
            recordDescriptors[0] = recDesc;
        }
    }

    @Override
    public IIndexDataflowHelperFactory getIndexDataflowHelperFactory() {
        return invertedIndexDataflowHelperFactory;
    }

    @Override
    public IFileSplitProvider getFileSplitProvider() {
        return btreeFileSplitProvider;
    }

    @Override
    public IFileSplitProvider getInvListsFileSplitProvider() {
        return invListsFileSplitProvider;
    }

    @Override
    public IBinaryComparatorFactory[] getTreeIndexComparatorFactories() {
        return tokenComparatorFactories;
    }

    @Override
    public ITypeTraits[] getTreeIndexTypeTraits() {
        return tokenTypeTraits;
    }

    @Override
    public IStorageManagerInterface getStorageManager() {
        return storageManager;
    }

    @Override
    public RecordDescriptor getRecordDescriptor() {
        return recordDescriptors[0];
    }

    @Override
    public IIndexLifecycleManagerProvider getLifecycleManagerProvider() {
        return lifecycleManagerProvider;
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

    @Override
    public boolean getRetainInput() {
        return retainInput;
    }

    @Override
    public IOperationCallbackProvider getOpCallbackProvider() {
        return opCallbackProvider;
    }

    @Override
    public ITupleFilterFactory getTupleFilterFactory() {
        return null;
    }
}