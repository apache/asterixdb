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
package edu.uci.ics.hyracks.storage.am.lsm.rtree.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTree;
import edu.uci.ics.hyracks.storage.am.rtree.dataflow.RTreeSearchOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SearchPredicate;
import edu.uci.ics.hyracks.storage.am.rtree.util.RTreeUtils;

public class LSMRTreeSearchOperatorNodePushable extends RTreeSearchOperatorNodePushable {

    public LSMRTreeSearchOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, IRecordDescriptorProvider recordDescProvider, int[] keyFields) {
        super(opDesc, ctx, partition, recordDescProvider, keyFields);
    }

    @Override
    protected ISearchPredicate createSearchPredicate() {
        LSMRTree lsmRtree = (LSMRTree) treeIndex;
        cmp = RTreeUtils.getSearchMultiComparator(lsmRtree.getComparatorFactories(), searchKey);
        return new SearchPredicate(searchKey, cmp);
    }
}