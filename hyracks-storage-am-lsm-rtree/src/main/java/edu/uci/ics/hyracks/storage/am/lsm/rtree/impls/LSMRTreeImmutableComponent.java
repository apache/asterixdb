package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AbstractImmutableLSMComponent;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;

public class LSMRTreeImmutableComponent extends AbstractImmutableLSMComponent {
    private final RTree rtree;
    private final BTree btree;

    public LSMRTreeImmutableComponent(RTree rtree, BTree btree) {
        this.rtree = rtree;
        this.btree = btree;
    }

    @Override
    public void destroy() throws HyracksDataException {
        rtree.deactivate();
        rtree.destroy();
        if (btree != null) {
            btree.deactivate();
            btree.destroy();
        }
    }

    public RTree getRTree() {
        return rtree;
    }

    public BTree getBTree() {
        return btree;
    }

}
