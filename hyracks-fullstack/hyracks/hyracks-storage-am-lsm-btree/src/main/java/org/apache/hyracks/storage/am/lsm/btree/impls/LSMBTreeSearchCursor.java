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
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;

public class LSMBTreeSearchCursor implements IIndexCursor {

    public enum LSMBTreeSearchType {
        POINT,
        RANGE
    }

    private final LSMBTreePointSearchCursor pointCursor;
    private final LSMBTreeRangeSearchCursor rangeCursor;
    private final LSMBTreeDiskComponentScanCursor scanCursor;
    private IIndexCursor currentCursor;

    public LSMBTreeSearchCursor(ILSMIndexOperationContext opCtx) {
        pointCursor = new LSMBTreePointSearchCursor(opCtx);
        rangeCursor = new LSMBTreeRangeSearchCursor(opCtx);
        scanCursor = new LSMBTreeDiskComponentScanCursor(opCtx);
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMBTreeCursorInitialState lsmInitialState = (LSMBTreeCursorInitialState) initialState;
        RangePredicate btreePred = (RangePredicate) searchPred;

        currentCursor =
                btreePred.isPointPredicate(lsmInitialState.getOriginalKeyComparator()) ? pointCursor : rangeCursor;
        currentCursor.open(lsmInitialState, searchPred);
    }

    public void scan(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        currentCursor = scanCursor;
        currentCursor.open(initialState, searchPred);
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        return currentCursor.hasNext();
    }

    @Override
    public void next() throws HyracksDataException {
        currentCursor.next();
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (currentCursor != null) {
            currentCursor.destroy();
        }
        currentCursor = null;
    }

    @Override
    public void close() throws HyracksDataException {
        if (currentCursor != null) {
            currentCursor.close();
        }
        currentCursor = null;
    }

    @Override
    public ITupleReference getTuple() {
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
}
