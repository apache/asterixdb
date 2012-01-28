package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;

public class LSMRTreeBulkLoadContext implements IIndexBulkLoadContext {
    private final RTree rtree;
    private final BTree btree;
    private IIndexBulkLoadContext bulkLoadCtx;

    public LSMRTreeBulkLoadContext(RTree rtree, BTree btree) {
        this.rtree = rtree;
        this.btree = btree;
    }

    public void beginBulkLoad(float fillFactor) throws HyracksDataException, TreeIndexException {
        bulkLoadCtx = rtree.beginBulkLoad(fillFactor);
    }

    public RTree getRTree() {
        return rtree;
    }

    public BTree getBTree() {
        return btree;
    }

    public IIndexBulkLoadContext getBulkLoadCtx() {
        return bulkLoadCtx;
    }
}