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
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class LSMBTreeSearchCursor implements ITreeIndexCursor {

    public enum LSMBTreeSearchType {
        POINT,
        RANGE
    }

    private final LSMBTreePointSearchCursor pointCursor;
    private final LSMBTreeRangeSearchCursor rangeCursor;
    private ITreeIndexCursor currentCursor;

    public LSMBTreeSearchCursor(ILSMIndexOperationContext opCtx) {
        pointCursor = new LSMBTreePointSearchCursor(opCtx);
        rangeCursor = new LSMBTreeRangeSearchCursor(opCtx);
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMBTreeCursorInitialState lsmInitialState = (LSMBTreeCursorInitialState) initialState;
        RangePredicate btreePred = (RangePredicate) searchPred;
        currentCursor =
                btreePred.isPointPredicate(lsmInitialState.getOriginalKeyComparator()) ? pointCursor : rangeCursor;
        currentCursor.open(lsmInitialState, searchPred);
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
    public void close() throws HyracksDataException {
        if (currentCursor != null) {
            currentCursor.close();
        }
        currentCursor = null;
    }

    @Override
    public void reset() throws HyracksDataException {
        if (currentCursor != null) {
            currentCursor.reset();
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

    @Override
    public ICachedPage getPage() {
        return currentCursor.getPage();
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        currentCursor.setBufferCache(bufferCache);
    }

    @Override
    public void setFileId(int fileId) {
        currentCursor.setFileId(fileId);

    }

    @Override
    public boolean exclusiveLatchNodes() {
        return currentCursor.exclusiveLatchNodes();
    }

    @Override
    public void markCurrentTupleAsUpdated() throws HyracksDataException {
        throw new HyracksDataException("Updating tuples is not supported with this cursor.");
    }
}
