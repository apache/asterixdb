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
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;
import org.apache.hyracks.storage.am.rtree.util.RTreeUtils;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;

public class RTreeSearchOperatorNodePushable extends IndexSearchOperatorNodePushable {
    protected PermutingFrameTupleReference searchKey;
    protected MultiComparator cmp;

    public RTreeSearchOperatorNodePushable(IHyracksTaskContext ctx, int partition, RecordDescriptor inputRecDesc,
            int[] keyFields, int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes,
            IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput, boolean retainMissing,
            IMissingWriterFactory missingWriterFactory, ISearchOperationCallbackFactory searchCallbackFactory,
            boolean appendIndexFilter) throws HyracksDataException {
        this(ctx, partition, inputRecDesc, keyFields, minFilterFieldIndexes, maxFilterFieldIndexes, indexHelperFactory,
                retainInput, retainMissing, missingWriterFactory, searchCallbackFactory, appendIndexFilter, false, null,
                null);
    }

    public RTreeSearchOperatorNodePushable(IHyracksTaskContext ctx, int partition, RecordDescriptor inputRecDesc,
            int[] keyFields, int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes,
            IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput, boolean retainMissing,
            IMissingWriterFactory missingWriterFactory, ISearchOperationCallbackFactory searchCallbackFactory,
            boolean appendIndexFilter, boolean appendOpCallbackProceedResult,
            byte[] searchCallbackProceedResultFalseValue, byte[] searchCallbackProceedResultTrueValue)
            throws HyracksDataException {
        // TODO: predicate & limit pushdown not enabled for RTree yet
        super(ctx, inputRecDesc, partition, minFilterFieldIndexes, maxFilterFieldIndexes, indexHelperFactory,
                retainInput, retainMissing, missingWriterFactory, searchCallbackFactory, appendIndexFilter, null, -1,
                appendOpCallbackProceedResult, searchCallbackProceedResultFalseValue,
                searchCallbackProceedResultTrueValue);
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
        return ((ITreeIndex) index).getFieldCount();
    }

    @Override
    protected void addAdditionalIndexAccessorParams(IIndexAccessParameters iap) throws HyracksDataException {
        // no additional parameteres are required for the B+Tree search case yet
    }

}
