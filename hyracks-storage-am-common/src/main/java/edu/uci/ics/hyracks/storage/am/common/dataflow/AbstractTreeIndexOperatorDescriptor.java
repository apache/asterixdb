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

package edu.uci.ics.hyracks.storage.am.common.dataflow;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public abstract class AbstractTreeIndexOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor implements
        ITreeIndexOperatorDescriptorHelper {

    private static final long serialVersionUID = 1L;

    protected final IFileSplitProvider fileSplitProvider;

    protected final IBinaryComparatorFactory[] comparatorFactories;

    protected final ITreeIndexFrameFactory interiorFrameFactory;
    protected final ITreeIndexFrameFactory leafFrameFactory;

    protected final IStorageManagerInterface storageManager;
    protected final IIndexRegistryProvider<ITreeIndex> treeIndexRegistryProvider;

    protected final ITypeTrait[] typeTraits;

    protected final ITreeIndexOpHelperFactory opHelperFactory;

    public AbstractTreeIndexOperatorDescriptor(JobSpecification spec, int inputArity, int outputArity,
            RecordDescriptor recDesc, IStorageManagerInterface storageManager,
            IIndexRegistryProvider<ITreeIndex> treeIndexRegistryProvider, IFileSplitProvider fileSplitProvider,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            ITypeTrait[] typeTraits, IBinaryComparatorFactory[] comparatorFactories,
            ITreeIndexOpHelperFactory opHelperFactory) {
        super(spec, inputArity, outputArity);
        this.fileSplitProvider = fileSplitProvider;
        this.storageManager = storageManager;
        this.treeIndexRegistryProvider = treeIndexRegistryProvider;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.typeTraits = typeTraits;
        this.comparatorFactories = comparatorFactories;
        this.opHelperFactory = opHelperFactory;
        if (outputArity > 0)
            recordDescriptors[0] = recDesc;
    }

    @Override
    public IFileSplitProvider getTreeIndexFileSplitProvider() {
        return fileSplitProvider;
    }

    @Override
    public IBinaryComparatorFactory[] getTreeIndexComparatorFactories() {
        return comparatorFactories;
    }

    @Override
    public ITypeTrait[] getTreeIndexTypeTraits() {
        return typeTraits;
    }

    @Override
    public ITreeIndexFrameFactory getTreeIndexInteriorFactory() {
        return interiorFrameFactory;
    }

    @Override
    public ITreeIndexFrameFactory getTreeIndexLeafFactory() {
        return leafFrameFactory;
    }

    @Override
    public IStorageManagerInterface getStorageManager() {
        return storageManager;
    }

    @Override
    public IIndexRegistryProvider<ITreeIndex> getTreeIndexRegistryProvider() {
        return treeIndexRegistryProvider;
    }

    @Override
    public RecordDescriptor getRecordDescriptor() {
        return recordDescriptors[0];
    }

    @Override
    public ITreeIndexOpHelperFactory getTreeIndexOpHelperFactory() {
        return opHelperFactory;
    }
}
