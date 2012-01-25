package edu.uci.ics.hyracks.storage.am.lsmtree.common.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;

public class LSMTreeBulkLoadContext implements IIndexBulkLoadContext {
    private final ITreeIndex tree;
    private IIndexBulkLoadContext bulkLoadCtx;

    public LSMTreeBulkLoadContext(ITreeIndex tree) {
        this.tree = tree;
    }

    public void beginBulkLoad(float fillFactor) throws HyracksDataException, TreeIndexException {
        bulkLoadCtx = tree.beginBulkLoad(fillFactor);
    }

    public ITreeIndex getTree() {
        return tree;
    }

    public IIndexBulkLoadContext getBulkLoadCtx() {
        return bulkLoadCtx;
    }
}