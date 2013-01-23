package edu.uci.ics.hyracks.storage.am.lsm.btree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AbstractImmutableLSMComponent;

public class LSMBTreeImmutableComponent extends AbstractImmutableLSMComponent {

    private final BTree btree;

    public LSMBTreeImmutableComponent(BTree btree) {
        this.btree = btree;
    }

    @Override
    public void destroy() throws HyracksDataException {
        btree.deactivate();
        btree.destroy();
    }

    public BTree getBTree() {
        return btree;
    }
}
