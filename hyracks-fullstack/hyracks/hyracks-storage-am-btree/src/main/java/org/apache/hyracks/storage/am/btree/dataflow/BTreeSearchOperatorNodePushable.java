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
package org.apache.hyracks.storage.am.btree.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexSearchOperatorNodePushable;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;

public class BTreeSearchOperatorNodePushable extends IndexSearchOperatorNodePushable {
    protected final boolean lowKeyInclusive;
    protected final boolean highKeyInclusive;

    protected PermutingFrameTupleReference lowKey;
    protected PermutingFrameTupleReference highKey;
    protected MultiComparator lowKeySearchCmp;
    protected MultiComparator highKeySearchCmp;

    public BTreeSearchOperatorNodePushable(IHyracksTaskContext ctx, int partition, RecordDescriptor inputRecDesc,
            int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive,
            int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes, IIndexDataflowHelperFactory indexHelperFactory,
            boolean retainInput, boolean retainMissing, IMissingWriterFactory missingWriterFactory,
            ISearchOperationCallbackFactory searchCallbackFactory, boolean appendIndexFilter)
            throws HyracksDataException {
        this(ctx, partition, inputRecDesc, lowKeyFields, highKeyFields, lowKeyInclusive, highKeyInclusive,
                minFilterFieldIndexes, maxFilterFieldIndexes, indexHelperFactory, retainInput, retainMissing,
                missingWriterFactory, searchCallbackFactory, appendIndexFilter, null, -1, false, null, null);
    }

    public BTreeSearchOperatorNodePushable(IHyracksTaskContext ctx, int partition, RecordDescriptor inputRecDesc,
            int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive,
            int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes, IIndexDataflowHelperFactory indexHelperFactory,
            boolean retainInput, boolean retainMissing, IMissingWriterFactory missingWriterFactory,
            ISearchOperationCallbackFactory searchCallbackFactory, boolean appendIndexFilter,
            ITupleFilterFactory tupleFilterFactory, long outputLimit, boolean appendOpCallbackProceedResult,
            byte[] searchCallbackProceedResultFalseValue, byte[] searchCallbackProceedResultTrueValue)
            throws HyracksDataException {
        super(ctx, inputRecDesc, partition, minFilterFieldIndexes, maxFilterFieldIndexes, indexHelperFactory,
                retainInput, retainMissing, missingWriterFactory, searchCallbackFactory, appendIndexFilter,
                tupleFilterFactory, outputLimit, appendOpCallbackProceedResult, searchCallbackProceedResultFalseValue,
                searchCallbackProceedResultTrueValue);
        this.lowKeyInclusive = lowKeyInclusive;
        this.highKeyInclusive = highKeyInclusive;
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
        if (minFilterKey != null) {
            minFilterKey.reset(accessor, tupleIndex);
        }
        if (maxFilterKey != null) {
            maxFilterKey.reset(accessor, tupleIndex);
        }
    }

    @Override
    protected ISearchPredicate createSearchPredicate() {
        ITreeIndex treeIndex = (ITreeIndex) index;
        lowKeySearchCmp = BTreeUtils.getSearchMultiComparator(treeIndex.getComparatorFactories(), lowKey);
        highKeySearchCmp = BTreeUtils.getSearchMultiComparator(treeIndex.getComparatorFactories(), highKey);
        return new RangePredicate(lowKey, highKey, lowKeyInclusive, highKeyInclusive, lowKeySearchCmp, highKeySearchCmp,
                minFilterKey, maxFilterKey);
    }

    @Override
    protected int getFieldCount() {
        return ((ITreeIndex) index).getFieldCount();
    }

    @Override
    protected void addAdditionalIndexAccessorParams(IIndexAccessParameters iap) throws HyracksDataException {
        // No additional parameters are required for the B+Tree search case
    }

}
