package edu.uci.ics.hyracks.storage.am.rtree.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.dataflow.ITreeIndexOpHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.ITreeIndexOperatorDescriptorHelper;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexHelperOpenMode;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexOpHelper;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeDiskOrderScanCursor;

public class RTreeOpHelperFactory implements ITreeIndexOpHelperFactory {
    
    private static final long serialVersionUID = 1L;

    @Override
    public TreeIndexOpHelper createTreeIndexOpHelper(ITreeIndexOperatorDescriptorHelper opDesc,
            IHyracksStageletContext ctx, int partition, IndexHelperOpenMode mode) {        
        return new RTreeOpHelper(opDesc, ctx, partition, mode);
    }
    
    public ITreeIndexCursor createDiskOrderScanCursor(ITreeIndexFrame leafFrame) throws HyracksDataException {
        return new RTreeDiskOrderScanCursor((IRTreeFrame)leafFrame);
    }
}
