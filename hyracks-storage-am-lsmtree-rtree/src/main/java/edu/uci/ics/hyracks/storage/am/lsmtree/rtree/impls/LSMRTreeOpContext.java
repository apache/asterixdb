package edu.uci.ics.hyracks.storage.am.lsmtree.rtree.impls;

import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeOpContext;

public final class LSMRTreeOpContext extends RTreeOpContext {
    
    public final RTree.RTreeAccessor memRtreeAccessor;

    public LSMRTreeOpContext(RTree.RTreeAccessor memRtreeAccessor, IRTreeLeafFrame leafFrame,
            IRTreeInteriorFrame interiorFrame, ITreeIndexMetaDataFrame metaFrame, int treeHeightHint) {

        super(leafFrame, interiorFrame, metaFrame, treeHeightHint);

        this.memRtreeAccessor = memRtreeAccessor;
        // Overwrite the RTree accessor's op context with our LSMRTreeOpContext.
        this.memRtreeAccessor.setOpContext(this);

        reset(op);
    }

    @Override
    public void reset(IndexOp newOp) {
        super.reset(newOp);
    }

}