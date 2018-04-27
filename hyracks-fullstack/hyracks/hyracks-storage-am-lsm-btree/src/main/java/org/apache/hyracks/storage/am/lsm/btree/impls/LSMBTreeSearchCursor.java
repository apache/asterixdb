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
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ILSMIndexCursor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchPredicate;

public class LSMBTreeSearchCursor extends EnforcedIndexCursor implements ILSMIndexCursor {

    public enum LSMBTreeSearchType {
        POINT,
        RANGE
    }

    private final LSMBTreePointSearchCursor pointCursor;
    private final LSMBTreeRangeSearchCursor rangeCursor;
    private final LSMBTreeDiskComponentScanCursor scanCursor;
    private ILSMIndexCursor currentCursor;

    public LSMBTreeSearchCursor(ILSMIndexOperationContext opCtx) {
        pointCursor = new LSMBTreePointSearchCursor(opCtx);
        rangeCursor = new LSMBTreeRangeSearchCursor(opCtx);
        scanCursor = new LSMBTreeDiskComponentScanCursor(opCtx);
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMBTreeCursorInitialState lsmInitialState = (LSMBTreeCursorInitialState) initialState;
        RangePredicate btreePred = (RangePredicate) searchPred;
        currentCursor = lsmInitialState.isDiskComponentScan() ? scanCursor
                : btreePred.isPointPredicate(lsmInitialState.getOriginalKeyComparator()) ? pointCursor : rangeCursor;
        currentCursor.open(lsmInitialState, searchPred);
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        return currentCursor.hasNext();
    }

    @Override
    public void doNext() throws HyracksDataException {
        currentCursor.next();
    }

    @Override
    public void doDestroy() throws HyracksDataException {
        try {
            pointCursor.destroy();
        } finally {
            try {
                rangeCursor.destroy();
            } finally {
                try {
                    scanCursor.destroy();
                } finally {
                    currentCursor = null;
                }
            }
        }
    }

    @Override
    public void doClose() throws HyracksDataException {
        if (currentCursor != null) {
            currentCursor.close();
        }
        currentCursor = null;
    }

    @Override
    public ITupleReference doGetTuple() {
        return currentCursor.getTuple();
    }

    @Override
    public ITupleReference getFilterMinTuple() {
        return currentCursor.getFilterMinTuple();
    }

    @Override
    public ITupleReference getFilterMaxTuple() {
        return currentCursor.getFilterMaxTuple();
    }

    @Override
    public boolean getSearchOperationCallbackProceedResult() {
        return currentCursor.getSearchOperationCallbackProceedResult();
    }
}
