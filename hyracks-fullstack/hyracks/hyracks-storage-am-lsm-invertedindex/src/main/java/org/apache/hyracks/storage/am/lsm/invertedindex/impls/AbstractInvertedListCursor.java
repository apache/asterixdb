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

package org.apache.hyracks.storage.am.lsm.invertedindex.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchPredicate;

/**
 * A cursor that reads an inverted list.
 */
public abstract class AbstractInvertedListCursor extends EnforcedIndexCursor implements IInvertedListCursor {

    /**
     * Opens an inverted list cursor.
     */
    protected void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        // If the given cursor state has page ids and the number of elements for the given inverted list,
        // this should be set. Otherwise (for in-memory cursor), doesn't need to do anything.
        int invListStartPageId = LSMInvertedIndexSearchCursorInitialState.INVALID_VALUE;
        int invListEndPageId = LSMInvertedIndexSearchCursorInitialState.INVALID_VALUE;
        int invListStartOffset = LSMInvertedIndexSearchCursorInitialState.INVALID_VALUE;
        int invListNumElements = LSMInvertedIndexSearchCursorInitialState.INVALID_VALUE;
        if (initialState instanceof LSMInvertedIndexSearchCursorInitialState) {
            LSMInvertedIndexSearchCursorInitialState invIndexInitialState =
                    (LSMInvertedIndexSearchCursorInitialState) initialState;
            invListStartPageId = invIndexInitialState.getInvListStartPageId();
            invListEndPageId = invIndexInitialState.getInvListEndPageId();
            invListStartOffset = invIndexInitialState.getInvListStartOffset();
            invListNumElements = invIndexInitialState.getInvListNumElements();
        }
        if (invListNumElements != LSMInvertedIndexSearchCursorInitialState.INVALID_VALUE) {
            setInvListInfo(invListStartPageId, invListEndPageId, invListStartOffset, invListNumElements);
        }
    }

    protected abstract void setInvListInfo(int startPageId, int endPageId, int startOff, int numElements)
            throws HyracksDataException;
}
