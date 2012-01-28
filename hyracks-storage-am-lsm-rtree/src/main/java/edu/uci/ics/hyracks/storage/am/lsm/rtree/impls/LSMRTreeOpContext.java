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
    }

    @Override
    public void reset() {

    }

}