package edu.uci.ics.hyracks.storage.am.lsm.btree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AbstractImmutableLSMComponent;

public class LSMBTreeImmutableComponent extends AbstractImmutableLSMComponent {

    private final BTree btree;
    private final BloomFilter bloomFilter;

    public LSMBTreeImmutableComponent(BTree btree, BloomFilter bloomFilter) {
        this.btree = btree;
        this.bloomFilter = bloomFilter;
    }

    @Override
    public void destroy() throws HyracksDataException {
        btree.deactivate();
        btree.destroy();
        bloomFilter.deactivate();
        bloomFilter.destroy();
    }

    public BTree getBTree() {
        return btree;
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }
}
