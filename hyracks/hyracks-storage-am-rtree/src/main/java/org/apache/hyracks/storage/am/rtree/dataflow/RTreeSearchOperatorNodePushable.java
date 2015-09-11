/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.storage.am.rtree.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IndexSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.common.tuples.PermutingFrameTupleReference;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;
import org.apache.hyracks.storage.am.rtree.util.RTreeUtils;

public class RTreeSearchOperatorNodePushable extends IndexSearchOperatorNodePushable {
    protected PermutingFrameTupleReference searchKey;
    protected MultiComparator cmp;

    public RTreeSearchOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, IRecordDescriptorProvider recordDescProvider, int[] keyFields, int[] minFilterFieldIndexes,
            int[] maxFilterFieldIndexes) {
        super(opDesc, ctx, partition, recordDescProvider, minFilterFieldIndexes, maxFilterFieldIndexes);
        if (keyFields != null && keyFields.length > 0) {
            searchKey = new PermutingFrameTupleReference();
            searchKey.setFieldPermutation(keyFields);
        }
    }

    @Override
    protected ISearchPredicate createSearchPredicate() {
        ITreeIndex treeIndex = (ITreeIndex) index;
        cmp = RTreeUtils.getSearchMultiComparator(treeIndex.getComparatorFactories(), searchKey);
        return new SearchPredicate(searchKey, cmp, minFilterKey, maxFilterKey);
    }

    @Override
    protected void resetSearchPredicate(int tupleIndex) {
        if (searchKey != null) {
            searchKey.reset(accessor, tupleIndex);
        }
        if (minFilterKey != null) {
            minFilterKey.reset(accessor, tupleIndex);
        }
        if (maxFilterKey != null) {
            maxFilterKey.reset(accessor, tupleIndex);
        }
    }

    @Override
    protected int getFieldCount() {
        return ((ITreeIndex)index).getFieldCount();
    }
}