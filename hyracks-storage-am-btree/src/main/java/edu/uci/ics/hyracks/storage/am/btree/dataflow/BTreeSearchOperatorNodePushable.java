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

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.PermutingFrameTupleReference;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexSearchOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class BTreeSearchOperatorNodePushable extends TreeIndexSearchOperatorNodePushable {
    protected PermutingFrameTupleReference lowKey;
    protected PermutingFrameTupleReference highKey;
    protected boolean lowKeyInclusive;
    protected boolean highKeyInclusive;
    protected MultiComparator lowKeySearchCmp;
    protected MultiComparator highKeySearchCmp;

    public BTreeSearchOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, IRecordDescriptorProvider recordDescProvider, int[] lowKeyFields, int[] highKeyFields,
            boolean lowKeyInclusive, boolean highKeyInclusive) {
        super(opDesc, ctx, partition, recordDescProvider);
        this.lowKeyInclusive = lowKeyInclusive;
        this.highKeyInclusive = highKeyInclusive;
        this.recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getOperatorId(), 0);
        if (lowKeyFields != null && lowKeyFields.length > 0) {
            lowKey = new PermutingFrameTupleReference();
            lowKey.setFieldPermutation(lowKeyFields);
        }
        if (highKeyFields != null && highKeyFields.length > 0) {
            highKey = new PermutingFrameTupleReference();
            highKey.setFieldPermutation(highKeyFields);
        }
    }

    @Override
    protected void resetSearchPredicate(int tupleIndex) {
        if (lowKey != null) {
            lowKey.reset(accessor, tupleIndex);
        }
        if (highKey != null) {
            highKey.reset(accessor, tupleIndex);
        }
    }

    @Override
    protected ISearchPredicate createSearchPredicate() {
        lowKeySearchCmp = BTreeUtils.getSearchMultiComparator(treeIndex.getComparatorFactories(), lowKey);
        highKeySearchCmp = BTreeUtils.getSearchMultiComparator(treeIndex.getComparatorFactories(), highKey);
        return new RangePredicate(lowKey, highKey, lowKeyInclusive, highKeyInclusive, lowKeySearchCmp, highKeySearchCmp);
    }
}