/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IntArrayList;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.LongArrayList;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public final class BTreeOpContext implements IIndexOpContext {
    private final int INIT_ARRAYLIST_SIZE = 6;    
    public final IBTreeLeafFrame leafFrame;
    public final IBTreeInteriorFrame interiorFrame;
    public final ITreeIndexMetaDataFrame metaFrame;
    public IndexOp op;
    public ITreeIndexCursor cursor;
    public BTreeCursorInitialState cursorInitialState;
    public RangePredicate pred;
    public BTreeSplitKey splitKey;
    public int opRestarts = 0;
    public LongArrayList pageLsns;
    public IntArrayList smPages;
    public IntArrayList freePages;

    public BTreeOpContext(IndexOp op, IBTreeLeafFrame leafFrame, IBTreeInteriorFrame interiorFrame,
            ITreeIndexMetaDataFrame metaFrame, MultiComparator cmp) {        
        if (leafFrame != null) {
        	leafFrame.setMultiComparator(cmp);
        }
        this.leafFrame = leafFrame;
        if (interiorFrame != null) {
        	interiorFrame.setMultiComparator(cmp);
        }
        this.interiorFrame = interiorFrame;
        this.metaFrame = metaFrame;
        this.pageLsns = new LongArrayList(INIT_ARRAYLIST_SIZE, INIT_ARRAYLIST_SIZE);
        reset(op);
    }

    public void reset() {
        if (pageLsns != null)
            pageLsns.clear();
        if (freePages != null)
            freePages.clear();
        if (smPages != null)
            smPages.clear();
        opRestarts = 0;
    }

    @Override
    public void reset(IndexOp newOp) {
        if (newOp == IndexOp.SEARCH || newOp == IndexOp.DISKORDERSCAN) {
            if (cursorInitialState == null) {
                cursorInitialState = new BTreeCursorInitialState(null);
            }
        } else {
            // Insert, update or delete operation.
            if (smPages == null) {
                smPages = new IntArrayList(INIT_ARRAYLIST_SIZE, INIT_ARRAYLIST_SIZE);
            }
            if (freePages == null) {
                freePages = new IntArrayList(INIT_ARRAYLIST_SIZE, INIT_ARRAYLIST_SIZE);
            }
            if (pred == null) {
                pred = new RangePredicate(true, null, null, true, true, null, null);
            }
            if (splitKey == null) {
                splitKey = new BTreeSplitKey(leafFrame.getTupleWriter().createTupleReference());
            }
        }
        this.op = newOp;
    }
}
