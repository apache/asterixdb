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

package org.apache.hyracks.storage.am.lsm.invertedindex.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IndexSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.InvertedIndexSearchPredicate;

public class LSMInvertedIndexSearchOperatorNodePushable extends IndexSearchOperatorNodePushable {

    protected final IInvertedIndexSearchModifier searchModifier;
    protected final int queryFieldIndex;
    protected final int invListFields;

    public LSMInvertedIndexSearchOperatorNodePushable(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, IRecordDescriptorProvider recordDescProvider, int queryFieldIndex,
            IInvertedIndexSearchModifier searchModifier, int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes)
            throws HyracksDataException {
        super(opDesc, ctx, partition, recordDescProvider, minFilterFieldIndexes, maxFilterFieldIndexes);
        this.searchModifier = searchModifier;
        this.queryFieldIndex = queryFieldIndex;
        // If retainInput is true, the frameTuple is created in IndexSearchOperatorNodePushable.open().
        if (!opDesc.getRetainInput()) {
            this.frameTuple = new FrameTupleReference();
        }
        AbstractLSMInvertedIndexOperatorDescriptor invIndexOpDesc = (AbstractLSMInvertedIndexOperatorDescriptor) opDesc;
        invListFields = invIndexOpDesc.getInvListsTypeTraits().length;
    }

    @Override
    protected ISearchPredicate createSearchPredicate() {
        AbstractLSMInvertedIndexOperatorDescriptor invIndexOpDesc = (AbstractLSMInvertedIndexOperatorDescriptor) opDesc;
        return new InvertedIndexSearchPredicate(invIndexOpDesc.getTokenizerFactory().createTokenizer(), searchModifier,
                minFilterKey, maxFilterKey);
    }

    @Override
    protected void resetSearchPredicate(int tupleIndex) {
        frameTuple.reset(accessor, tupleIndex);
        InvertedIndexSearchPredicate invIndexSearchPred = (InvertedIndexSearchPredicate) searchPred;
        invIndexSearchPred.setQueryTuple(frameTuple);
        invIndexSearchPred.setQueryFieldIndex(queryFieldIndex);
        if (minFilterKey != null) {
            minFilterKey.reset(accessor, tupleIndex);
        }
        if (maxFilterKey != null) {
            maxFilterKey.reset(accessor, tupleIndex);
        }
    }

    @Override
    protected int getFieldCount() {
        return invListFields;
    }
}
