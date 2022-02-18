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

package org.apache.hyracks.storage.am.lsm.btree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.BatchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;

/**
 * This cursor performs point searches for each batch of search keys.
 * Assumption: the search keys must be sorted into the increasing order.
 */
public class LSMBTreeBatchPointSearchCursor extends LSMBTreePointSearchCursor {

    public LSMBTreeBatchPointSearchCursor(ILSMIndexOperationContext opCtx) {
        super(opCtx);
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        BatchPredicate batchPred = (BatchPredicate) predicate;
        while (!foundTuple && batchPred.hasNext()) {
            batchPred.next();
            if (foundIn >= 0) {
                if (operationalComponents.get(foundIn).getType() == LSMComponentType.MEMORY) {
                    btreeCursors[foundIn].close();
                }
                foundIn = -1;
            }
            foundTuple = super.doHasNext();
        }
        return foundTuple;
    }

    @Override
    public void doNext() throws HyracksDataException {
        foundTuple = false;
    }

    @Override
    protected ITreeIndexCursor createCursor(LSMComponentType type, BTreeAccessor btreeAccessor) {
        return btreeAccessor.createPointCursor(false, true);
    }

    @Override
    protected boolean isSearchCandidate(int componentIndex) throws HyracksDataException {
        if (!super.isSearchCandidate(componentIndex)) {
            return false;
        }
        // check filters
        ITupleReference minFilterKey = predicate.getMinFilterTuple();
        ITupleReference maxFileterKey = predicate.getMaxFilterTuple();
        boolean filtered = minFilterKey != null && maxFileterKey != null;
        return !filtered || operationalComponents.get(componentIndex).getLSMComponentFilter().satisfy(minFilterKey,
                maxFileterKey, opCtx.getFilterCmp());
    }

    public int getKeyIndex() {
        return ((BatchPredicate) predicate).getKeyIndex();
    }

}
