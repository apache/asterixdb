/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk;

import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class OnDiskInvertedIndexOpContext implements IIndexOperationContext {

    public final RangePredicate btreePred = new RangePredicate(null, null, true, true, null, null);
    public IIndexAccessor btreeAccessor;
    public IIndexCursor btreeCursor;
    public MultiComparator searchCmp;
    // For prefix search on partitioned indexes.
    public MultiComparator prefixSearchCmp;

    public OnDiskInvertedIndexOpContext(BTree btree) {
        // TODO: Ignore opcallbacks for now.
        btreeAccessor = btree.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        btreeCursor = btreeAccessor.createSearchCursor();
        searchCmp = MultiComparator.createIgnoreFieldLength(btree.getComparatorFactories());
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
}
