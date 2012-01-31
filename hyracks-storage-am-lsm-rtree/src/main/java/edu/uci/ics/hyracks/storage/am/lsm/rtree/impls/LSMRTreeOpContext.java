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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeOpContext;

public final class LSMRTreeOpContext implements IIndexOpContext {

    private RTreeOpContext rtreeOpContext;
    private BTreeOpContext btreeOpContext;
    public final RTree.RTreeAccessor memRTreeAccessor;
    public final BTree.BTreeAccessor memBTreeAccessor;
    private IndexOp op;
    
    public LSMRTreeOpContext(RTree.RTreeAccessor memRtreeAccessor, IRTreeLeafFrame rtreeLeafFrame,
            IRTreeInteriorFrame rtreeInteriorFrame, ITreeIndexMetaDataFrame rtreeMetaFrame, int rTreeHeightHint,
            BTree.BTreeAccessor memBtreeAccessor, ITreeIndexFrameFactory btreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexMetaDataFrame btreeMetaFrame,
            MultiComparator btreeCmp) {

        this.memRTreeAccessor = memRtreeAccessor;
        this.memBTreeAccessor = memBtreeAccessor;
        this.rtreeOpContext = new RTreeOpContext(rtreeLeafFrame, rtreeInteriorFrame, rtreeMetaFrame, rTreeHeightHint);
        this.btreeOpContext = new BTreeOpContext(btreeLeafFrameFactory, btreeInteriorFrameFactory, btreeMetaFrame,
                btreeCmp);
    }

    public void reset(IndexOp newOp) {
        if (newOp == IndexOp.INSERT) {
            rtreeOpContext.reset(newOp);
        } else if (newOp == IndexOp.DELETE) {
            btreeOpContext.reset(IndexOp.INSERT);
        }
        this.op = newOp;
    }

    @Override
    public void reset() {

    }

    public IndexOp getIndexOp() {
    	return op;
    }
}