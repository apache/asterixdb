package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AbstractImmutableLSMComponent;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;

public class LSMRTreeImmutableComponent extends AbstractImmutableLSMComponent {
    private final RTree rtree;
    private final BTree btree;
    private final BloomFilter bloomFilter;

    public LSMRTreeImmutableComponent(RTree rtree, BTree btree, BloomFilter bloomFilter) {
        this.rtree = rtree;
        this.btree = btree;
        this.bloomFilter = bloomFilter;
    }

    @Override
    public void destroy() throws HyracksDataException {
        rtree.deactivate();
        rtree.destroy();
        if (btree != null) {
            btree.deactivate();
            btree.destroy();
            bloomFilter.deactivate();
            bloomFilter.destroy();
        }
    }

    public RTree getRTree() {
        return rtree;
    }

    public BTree getBTree() {
        return btree;
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }
}
