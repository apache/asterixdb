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

package edu.uci.ics.hyracks.storage.am.btree.dataflow;

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
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public abstract class AbstractBTreeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor implements
        IBTreeOperatorDescriptorHelper {

    private static final long serialVersionUID = 1L;

    protected final IFileSplitProvider fileSplitProvider;

    protected final IBinaryComparatorFactory[] comparatorFactories;

    protected final IBTreeInteriorFrameFactory interiorFrameFactory;
    protected final IBTreeLeafFrameFactory leafFrameFactory;

    protected final IStorageManagerInterface storageManager;
    protected final IIndexRegistryProvider<BTree> btreeRegistryProvider;

    protected final ITypeTrait[] typeTraits;

    public AbstractBTreeOperatorDescriptor(JobSpecification spec, int inputArity, int outputArity,
            RecordDescriptor recDesc, IStorageManagerInterface storageManager,
            IIndexRegistryProvider<BTree> btreeRegistryProvider, IFileSplitProvider fileSplitProvider,
            IBTreeInteriorFrameFactory interiorFactory, IBTreeLeafFrameFactory leafFactory, ITypeTrait[] typeTraits,
            IBinaryComparatorFactory[] comparatorFactories) {
        super(spec, inputArity, outputArity);
        this.fileSplitProvider = fileSplitProvider;
        this.storageManager = storageManager;
        this.btreeRegistryProvider = btreeRegistryProvider;
        this.interiorFrameFactory = interiorFactory;
        this.leafFrameFactory = leafFactory;
        this.typeTraits = typeTraits;
        this.comparatorFactories = comparatorFactories;
        if (outputArity > 0)
            recordDescriptors[0] = recDesc;
    }

    @Override
    public IFileSplitProvider getBTreeFileSplitProvider() {
        return fileSplitProvider;
    }

    @Override
    public IBinaryComparatorFactory[] getBTreeComparatorFactories() {
        return comparatorFactories;
    }

    @Override
    public ITypeTrait[] getBTreeTypeTraits() {
        return typeTraits;
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
}
