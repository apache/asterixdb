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

package edu.uci.ics.hyracks.storage.am.invertedindex.dataflow;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.util.InvertedIndexUtils;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public abstract class AbstractInvertedIndexOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor
        implements IInvertedIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    // General.
    protected final IStorageManagerInterface storageManager;
    protected final IIndexRegistryProvider<IIndex> indexRegistryProvider;
    protected final IOperationCallbackProvider opCallbackProvider;
    
    // Btree.
    protected final ITreeIndexFrameFactory btreeInteriorFrameFactory;
    protected final ITreeIndexFrameFactory btreeLeafFrameFactory;
    protected final ITypeTraits[] btreeTypeTraits;
    protected final IBinaryComparatorFactory[] btreeComparatorFactories;
    protected final IIndexDataflowHelperFactory btreeDataflowHelperFactory;
    protected final IFileSplitProvider btreeFileSplitProvider;

    // Inverted index.
    protected final ITypeTraits[] invListsTypeTraits;
    protected final IBinaryComparatorFactory[] invListComparatorFactories;
    protected final IBinaryTokenizerFactory tokenizerFactory;
    protected final IFileSplitProvider invListsFileSplitProvider;

    public AbstractInvertedIndexOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputArity, int outputArity,
            RecordDescriptor recDesc, IStorageManagerInterface storageManager,
            IFileSplitProvider btreeFileSplitProvider, IFileSplitProvider invListsFileSplitProvider,
            IIndexRegistryProvider<IIndex> indexRegistryProvider, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenComparatorFactories, ITypeTraits[] invListsTypeTraits,
            IBinaryComparatorFactory[] invListComparatorFactories, IBinaryTokenizerFactory tokenizerFactory,
            IIndexDataflowHelperFactory btreeDataflowHelperFactory, IOperationCallbackProvider opCallbackProvider) {
        super(spec, inputArity, outputArity);

        // General.
        this.storageManager = storageManager;
        this.indexRegistryProvider = indexRegistryProvider;
        this.opCallbackProvider = opCallbackProvider;
        
        // Btree.
        this.btreeTypeTraits = InvertedIndexUtils.getBTreeTypeTraits(tokenTypeTraits);
        ITreeIndexTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(btreeTypeTraits);
        this.btreeInteriorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        this.btreeLeafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        this.btreeComparatorFactories = tokenComparatorFactories;
        this.btreeDataflowHelperFactory = btreeDataflowHelperFactory;
        this.btreeFileSplitProvider = btreeFileSplitProvider;

        // Inverted index.
        this.invListsTypeTraits = invListsTypeTraits;
        this.invListComparatorFactories = invListComparatorFactories;
        this.tokenizerFactory = tokenizerFactory;
        this.invListsFileSplitProvider = invListsFileSplitProvider;

        if (outputArity > 0) {
            recordDescriptors[0] = recDesc;
        }
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
        return btreeComparatorFactories;
    }

    @Override
    public ITypeTraits[] getTreeIndexTypeTraits() {
        return btreeTypeTraits;
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
    public IIndexRegistryProvider<IIndex> getIndexRegistryProvider() {
        return indexRegistryProvider;
    }
    
    @Override
    public IIndexDataflowHelperFactory getIndexDataflowHelperFactory() {
        return btreeDataflowHelperFactory;
    }
    
    @Override
    public IOperationCallbackProvider getOpCallbackProvider() {
    	return opCallbackProvider;
    }
}