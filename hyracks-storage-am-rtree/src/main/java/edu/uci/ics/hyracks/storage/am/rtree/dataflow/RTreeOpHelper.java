package edu.uci.ics.hyracks.storage.am.rtree.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.ITreeIndexOperatorDescriptorHelper;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexHelperOpenMode;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexOpHelper;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class RTreeOpHelper extends TreeIndexOpHelper {

    protected MultiComparator interiorCmp;

    public RTreeOpHelper(ITreeIndexOperatorDescriptorHelper opDesc, IHyracksStageletContext ctx, int partition,
            IndexHelperOpenMode mode) {
        super(opDesc, ctx, partition, mode);
    }

    public ITreeIndex createTreeIndex() throws HyracksDataException {
        IBufferCache bufferCache = opDesc.getStorageManager().getBufferCache(ctx);
        ITreeIndexMetaDataFrameFactory metaDataFrameFactory = new LIFOMetaDataFrameFactory();
        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, indexFileId, 0,
                metaDataFrameFactory);

        return new RTree(bufferCache, freePageManager, opDesc.getTreeIndexInteriorFactory(),
                opDesc.getTreeIndexLeafFactory(), cmp);
    }
    
    public ITreeIndexCursor createDiskOrderScanCursor(ITreeIndexFrame leafFrame) throws HyracksDataException {
        throw new HyracksDataException("createDiskOrderScanCursor Operation not implemented.");
    }
}
