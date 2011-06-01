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
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public abstract class AbstractInvertedIndexOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor
        implements IInvertedIndexOperatorDescriptorHelper {

    private static final long serialVersionUID = 1L;

    // general
    protected final IStorageManagerInterface storageManager;

    // btree
    protected final IFileSplitProvider btreeFileSplitProvider;
    protected final IIndexRegistryProvider<BTree> btreeRegistryProvider;
    protected final IBTreeInteriorFrameFactory interiorFrameFactory;
    protected final IBTreeLeafFrameFactory leafFrameFactory;
    protected final ITypeTrait[] btreeTypeTraits;
    protected final IBinaryComparatorFactory[] btreeComparatorFactories;

    // inverted index
    protected final IFileSplitProvider invIndexFileSplitProvider;
    protected final IIndexRegistryProvider<InvertedIndex> invIndexRegistryProvider;    
    protected final ITypeTrait[] invIndexTypeTraits;
    protected final IBinaryComparatorFactory[] invIndexComparatorFactories;

    public AbstractInvertedIndexOperatorDescriptor(JobSpecification spec, int inputArity, int outputArity,
            RecordDescriptor recDesc, IStorageManagerInterface storageManager,
            IFileSplitProvider btreeFileSplitProvider, IIndexRegistryProvider<BTree> btreeRegistryProvider,
            IBTreeInteriorFrameFactory interiorFrameFactory, IBTreeLeafFrameFactory leafFrameFactory,
            ITypeTrait[] btreeTypeTraits, IBinaryComparatorFactory[] btreeComparatorFactories, float btreeFillFactor,
            IFileSplitProvider invIndexFileSplitProvider,
            IIndexRegistryProvider<InvertedIndex> invIndexRegistryProvider, ITypeTrait[] invIndexTypeTraits,
            IBinaryComparatorFactory[] invIndexComparatorFactories) {
        super(spec, inputArity, outputArity);

        // general
        this.storageManager = storageManager;

        // btree
        this.btreeFileSplitProvider = btreeFileSplitProvider;
        this.btreeRegistryProvider = btreeRegistryProvider;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.btreeTypeTraits = btreeTypeTraits;
        this.btreeComparatorFactories = btreeComparatorFactories;

        // inverted index
        this.invIndexFileSplitProvider = invIndexFileSplitProvider;
        this.invIndexRegistryProvider = invIndexRegistryProvider;
        this.invIndexTypeTraits = invIndexTypeTraits;
        this.invIndexComparatorFactories = invIndexComparatorFactories;

        if (outputArity > 0)
            recordDescriptors[0] = recDesc;
    }

    @Override
    public IFileSplitProvider getBTreeFileSplitProvider() {
        return btreeFileSplitProvider;
    }

    @Override
    public IBinaryComparatorFactory[] getBTreeComparatorFactories() {
        return btreeComparatorFactories;
    }

    @Override
    public ITypeTrait[] getBTreeTypeTraits() {
        return btreeTypeTraits;
    }

    @Override
    public IBTreeInteriorFrameFactory getBTreeInteriorFactory() {
        return interiorFrameFactory;
    }

    @Override
    public IBTreeLeafFrameFactory getBTreeLeafFactory() {
        return leafFrameFactory;
    }

    @Override
    public IStorageManagerInterface getStorageManager() {
        return storageManager;
    }

    @Override
    public IIndexRegistryProvider<BTree> getBTreeRegistryProvider() {
        return btreeRegistryProvider;
    }

    @Override
    public RecordDescriptor getRecordDescriptor() {
        return recordDescriptors[0];
    }

    @Override
    public IIndexRegistryProvider<InvertedIndex> getInvIndexRegistryProvider() {
        return invIndexRegistryProvider;
    }

    @Override
    public IBinaryComparatorFactory[] getInvIndexComparatorFactories() {
        return invIndexComparatorFactories;
    }

    @Override
    public IFileSplitProvider getInvIndexFileSplitProvider() {
        return invIndexFileSplitProvider;
    }

    @Override
    public ITypeTrait[] getInvIndexTypeTraits() {
        return invIndexTypeTraits;
    }
}
