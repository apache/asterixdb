package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AbstractImmutableLSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;

public class LSMInvertedIndexImmutableComponent extends AbstractImmutableLSMComponent {

    private final IInvertedIndex invIndex;
    private final BTree deletedKeysBTree;

    public LSMInvertedIndexImmutableComponent(IInvertedIndex invIndex, BTree deletedKeysBTree) {
        this.invIndex = invIndex;
        this.deletedKeysBTree = deletedKeysBTree;
    }

    @Override
    public void destroy() throws HyracksDataException {
        invIndex.deactivate();
        invIndex.destroy();
        deletedKeysBTree.deactivate();
        deletedKeysBTree.destroy();
    }

    public IInvertedIndex getInvIndex() {
        return invIndex;
    }

    public BTree getDeletedKeysBTree() {
        return deletedKeysBTree;
    }

}
