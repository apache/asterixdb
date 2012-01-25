package edu.uci.ics.hyracks.storage.am.lsmtree.rtree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.common.freepage.InMemoryFreePageManager;

public class LSMRTreeInMemoryFreePageManager extends InMemoryFreePageManager {

    public LSMRTreeInMemoryFreePageManager(int capacity, ITreeIndexMetaDataFrameFactory metaDataFrameFactory) {
        super(capacity, metaDataFrameFactory);
        // We start the currentPageId from 3, because the RTree uses
        // the first page as metadata page, and the second page as root page.
        // And the BTree uses the third page as metadata, and the third page as root page 
        // (when returning free pages we first increment, then get)
        currentPageId.set(3);
    }

    @Override
    public void init(ITreeIndexMetaDataFrame metaFrame, int currentMaxPage) throws HyracksDataException {
        currentPageId.set(3);
    }

    public int getCapacity() {
        return capacity - 4;
    }
    
    public void reset() {
        currentPageId.set(3);
    }
}
