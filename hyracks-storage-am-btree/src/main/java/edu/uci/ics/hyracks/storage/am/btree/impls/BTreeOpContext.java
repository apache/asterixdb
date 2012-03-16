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

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IntArrayList;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.LongArrayList;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class BTreeOpContext implements IIndexOpContext {
    private final int INIT_ARRAYLIST_SIZE = 6;
    public MultiComparator cmp;
    public ITreeIndexFrameFactory leafFrameFactory;
    public ITreeIndexFrameFactory interiorFrameFactory;
    public IBTreeLeafFrame leafFrame;
    public IBTreeInteriorFrame interiorFrame;
    public ITreeIndexMetaDataFrame metaFrame;
    public IndexOp op;
    public ITreeIndexCursor cursor;
    public BTreeCursorInitialState cursorInitialState;
    public RangePredicate pred;
    public BTreeSplitKey splitKey;    
    public LongArrayList pageLsns;
    public IntArrayList smPages;
    public IntArrayList freePages;
    public int opRestarts = 0;
    public boolean exceptionHandled;
    
    public BTreeOpContext(ITreeIndexFrameFactory leafFrameFactory, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexMetaDataFrame metaFrame, IBinaryComparatorFactory[] cmpFactories) {
        this.cmp = MultiComparator.create(cmpFactories);
        this.leafFrameFactory = leafFrameFactory;
        this.leafFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();
        if (leafFrame != null) {
            leafFrame.setMultiComparator(cmp);
        }
        this.interiorFrameFactory = interiorFrameFactory;
        this.interiorFrame = (IBTreeInteriorFrame) interiorFrameFactory.createFrame();
        if (interiorFrame != null) {
            interiorFrame.setMultiComparator(cmp);
        }
        this.metaFrame = metaFrame;
        this.pageLsns = new LongArrayList(INIT_ARRAYLIST_SIZE, INIT_ARRAYLIST_SIZE);
    }

    public void reset() {
        if (pageLsns != null)
            pageLsns.clear();
        if (freePages != null)
            freePages.clear();
        if (smPages != null)
            smPages.clear();
        opRestarts = 0;
        exceptionHandled = false;
    }

    @Override
    public void reset(IndexOp newOp) {
        if (newOp == IndexOp.SEARCH || newOp == IndexOp.DISKORDERSCAN) {
            if (cursorInitialState == null) {
                cursorInitialState = new BTreeCursorInitialState(null);
            }
        } else {
            // Insert, delete, update or upsert operation.
            if (smPages == null) {
                smPages = new IntArrayList(INIT_ARRAYLIST_SIZE, INIT_ARRAYLIST_SIZE);
            }
            if (freePages == null) {
                freePages = new IntArrayList(INIT_ARRAYLIST_SIZE, INIT_ARRAYLIST_SIZE);
            }
            if (pred == null) {
                pred = new RangePredicate(null, null, true, true, null, null);
            }
            if (splitKey == null) {
                splitKey = new BTreeSplitKey(leafFrame.getTupleWriter().createTupleReference());
            }
        }
        op = newOp;
        exceptionHandled = false;
    }

    public IBTreeLeafFrame createLeafFrame() {
        return (IBTreeLeafFrame) leafFrameFactory.createFrame();
    }

    public IBTreeInteriorFrame createInteriorFrame() {
        return (IBTreeInteriorFrame) interiorFrameFactory.createFrame();
    }
}
