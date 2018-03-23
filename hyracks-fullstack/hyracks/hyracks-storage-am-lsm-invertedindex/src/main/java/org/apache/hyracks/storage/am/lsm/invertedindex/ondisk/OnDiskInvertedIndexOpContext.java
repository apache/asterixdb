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

package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndexSearchCursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.MultiComparator;

public class OnDiskInvertedIndexOpContext implements IIndexOperationContext {

    private final RangePredicate btreePred = new RangePredicate(null, null, true, true, null, null);
    private final IIndexAccessor btreeAccessor;
    private final IIndexCursor btreeCursor;
    private final MultiComparator searchCmp;
    // For prefix search on partitioned indexes.
    private MultiComparator prefixSearchCmp;
    private boolean destroyed = false;
    private LSMInvertedIndexSearchCursorInitialState cursorInitialState;

    public OnDiskInvertedIndexOpContext(BTree btree) throws HyracksDataException {
        // TODO: Ignore opcallbacks for now.
        btreeAccessor = btree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        btreeCursor = btreeAccessor.createSearchCursor(false);
        searchCmp = MultiComparator.create(btree.getComparatorFactories());
        if (btree.getComparatorFactories().length > 1) {
            prefixSearchCmp = MultiComparator.create(btree.getComparatorFactories(), 0, 1);
        }
    }

    @Override
    public void reset() {
        // Nothing to be done here, only search operation supported.
    }

    @Override
    public void setOperation(IndexOperation newOp) {
        // Nothing to be done here, only search operation supported.
    }

    @Override
    public IndexOperation getOperation() {
        return IndexOperation.SEARCH;
    }

    public RangePredicate getBtreePred() {
        return btreePred;
    }

    public MultiComparator getSearchCmp() {
        return searchCmp;
    }

    public IIndexAccessor getBtreeAccessor() {
        return btreeAccessor;
    }

    public IIndexCursor getBtreeCursor() {
        return btreeCursor;
    }

    public MultiComparator getPrefixSearchCmp() {
        return prefixSearchCmp;
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (destroyed) {
            return;
        }
        destroyed = true;
        try {
            btreeAccessor.destroy();
        } finally {
            btreeCursor.destroy();
        }
    }

    public LSMInvertedIndexSearchCursorInitialState getCursorInitialState() {
        if (cursorInitialState == null) {
            cursorInitialState = new LSMInvertedIndexSearchCursorInitialState();
        }
        return cursorInitialState;
    }
}
