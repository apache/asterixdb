package edu.uci.ics.hyracks.storage.am.lsmtree.rtree.impls;

import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public final class LSMBTreeOpContext extends BTreeOpContext {

    public final BTree.BTreeAccessor memBTreeAccessor;

    public LSMBTreeOpContext(BTree.BTreeAccessor memBtreeAccessor, ITreeIndexFrameFactory leafFrameFactory,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexMetaDataFrame metaFrame, MultiComparator cmp) {
        super(leafFrameFactory, interiorFrameFactory, metaFrame, cmp);

        this.memBTreeAccessor = memBtreeAccessor;
        // Overwrite the BTree accessor's op context with our LSMBTreeOpContext.
        this.memBTreeAccessor.setOpContext(this);

        reset(op);
    }

    @Override
    public void reset(IndexOp newOp) {
        super.reset(newOp);
    }

}