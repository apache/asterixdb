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

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class InvertedIndexBulkLoadOperatorDescriptor extends AbstractInvertedIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final int[] fieldPermutation;
    private final float btreeFillFactor;
    private final IInvertedListBuilder invListBuilder;

    public InvertedIndexBulkLoadOperatorDescriptor(JobSpecification spec, IStorageManagerInterface storageManager,
            int[] fieldPermutation, IFileSplitProvider btreeFileSplitProvider,
            IIndexRegistryProvider<BTree> btreeRegistryProvider, IBTreeInteriorFrameFactory interiorFrameFactory,
            IBTreeLeafFrameFactory leafFrameFactory, ITypeTrait[] btreeTypeTraits,
            IBinaryComparatorFactory[] btreeComparatorFactories, float btreeFillFactor,
            IFileSplitProvider invIndexFileSplitProvider,
            IIndexRegistryProvider<InvertedIndex> invIndexRegistryProvider, ITypeTrait[] invIndexTypeTraits,
            IBinaryComparatorFactory[] invIndexComparatorFactories, IInvertedListBuilder invListBuilder) {
        super(spec, 1, 0, null, storageManager, btreeFileSplitProvider, btreeRegistryProvider, interiorFrameFactory,
                leafFrameFactory, btreeTypeTraits, btreeComparatorFactories, btreeFillFactor,
                invIndexFileSplitProvider, invIndexRegistryProvider, invIndexTypeTraits, invIndexComparatorFactories);
        this.fieldPermutation = fieldPermutation;
        this.btreeFillFactor = btreeFillFactor;
        this.invListBuilder = invListBuilder;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksStageletContext ctx, IOperatorEnvironment env,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new InvertedIndexBulkLoadOperatorNodePushable(this, ctx, partition, fieldPermutation, btreeFillFactor,
                invListBuilder, recordDescProvider);
    }
}
