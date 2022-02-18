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

package org.apache.hyracks.storage.am.btree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.api.IDiskBTreeStatefulPointSearchCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleMode;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public class DiskBTreePointSearchCursor extends DiskBTreeRangeSearchCursor
        implements IDiskBTreeStatefulPointSearchCursor {
    /**
     * A stateful cursor keeps the search state (last search page Id + index) across multiple searches
     * until {@link #clearSearchState()} is called explicity
     */
    private final boolean stateful;

    private boolean nextHasBeenCalled;

    private int lastPageId;
    private int lastTupleIndex = 0;

    public DiskBTreePointSearchCursor(IBTreeLeafFrame frame, boolean exclusiveLatchNodes, boolean stateful) {
        super(frame, exclusiveLatchNodes);
        this.stateful = stateful;
        lastPageId = IBufferCache.INVALID_PAGEID;
        lastTupleIndex = 0;
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        return tupleIndex >= 0 && !nextHasBeenCalled;
    }

    @Override
    public void doNext() throws HyracksDataException {
        nextHasBeenCalled = true;
    }

    @Override
    public void doClose() throws HyracksDataException {
        clearSearchState();
        super.doClose();
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        // in case open is called multiple times without closing
        if (page != null) {
            resetBeforeOpen();
        }
        accessor = ((BTreeCursorInitialState) initialState).getAccessor();
        searchCb = initialState.getSearchOperationCallback();
        originalKeyCmp = initialState.getOriginalKeyComparator();
        pageId = ((BTreeCursorInitialState) initialState).getPageId();
        page = initialState.getPage();
        isPageDirty = false;
        frame.setPage(page);
        setCursorToNextKey(searchPred);
    }

    @Override
    public int getLastPageId() {
        return lastPageId;
    }

    @Override
    protected int getLowKeyIndex() throws HyracksDataException {
        if (stateful) {
            return frame.findTupleIndex(lowKey, frameTuple, lowKeyCmp, lastTupleIndex);
        } else {
            return super.getLowKeyIndex();
        }
    }

    @Override
    public void setCursorToNextKey(ISearchPredicate searchPred) throws HyracksDataException {
        pred = (RangePredicate) searchPred;
        lowKeyCmp = pred.getLowKeyComparator();
        lowKey = pred.getLowKey();

        reusablePredicate.setLowKeyComparator(originalKeyCmp);

        lowKeyFtm = FindTupleMode.EXACT;
        lowKeyFtp = FindTupleNoExactMatchPolicy.NONE;

        nextHasBeenCalled = false;

        // only get the lowKey position
        tupleIndex = getLowKeyIndex();
        if (stateful) {
            lastPageId = pageId;
            if (tupleIndex >= 0) {
                lastTupleIndex = tupleIndex;
            } else {
                lastTupleIndex = -tupleIndex - 1;
            }
        }
    }

    private void clearSearchState() {
        this.lastPageId = IBufferCache.INVALID_PAGEID;
        this.lastTupleIndex = 0;
    }

    @Override
    public ITreeIndexFrame getFrame() {
        return frame;
    }
}
