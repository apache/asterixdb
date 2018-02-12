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
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleMode;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchPredicate;

public class DiskBTreePointSearchCursor extends DiskBTreeRangeSearchCursor {

    private boolean nextHasBeenCalled;

    public DiskBTreePointSearchCursor(IBTreeLeafFrame frame, boolean exclusiveLatchNodes) {
        super(frame, exclusiveLatchNodes);
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

        pred = (RangePredicate) searchPred;
        lowKeyCmp = pred.getLowKeyComparator();
        lowKey = pred.getLowKey();

        reusablePredicate.setLowKeyComparator(originalKeyCmp);

        lowKeyFtm = FindTupleMode.EXACT;
        lowKeyFtp = FindTupleNoExactMatchPolicy.NONE;

        nextHasBeenCalled = false;

        // only get the low key position
        tupleIndex = getLowKeyIndex();
    }

}
