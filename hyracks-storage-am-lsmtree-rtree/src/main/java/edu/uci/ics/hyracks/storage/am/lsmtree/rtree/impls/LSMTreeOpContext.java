package edu.uci.ics.hyracks.storage.am.lsmtree.rtree.impls;

import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;

public final class LSMTreeOpContext {

    public LSMRTreeOpContext LSMRTreeOpContext;
    public LSMBTreeOpContext LSMBTreeOpContext;

    public LSMTreeOpContext(LSMRTreeOpContext LSMRTreeOpContext, LSMBTreeOpContext LSMBTreeOpContext) {
        this.LSMRTreeOpContext = LSMRTreeOpContext;
        this.LSMBTreeOpContext = LSMBTreeOpContext;
    }
    
    public void reset(IndexOp newOp) {
        LSMRTreeOpContext.reset(newOp);
        LSMBTreeOpContext.reset(newOp);
    }

}