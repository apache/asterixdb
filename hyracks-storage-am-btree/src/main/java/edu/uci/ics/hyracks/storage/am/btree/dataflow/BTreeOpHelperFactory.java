package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeDiskOrderScanCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.dataflow.ITreeIndexOpHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.ITreeIndexOperatorDescriptorHelper;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexHelperOpenMode;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexOpHelper;

public class BTreeOpHelperFactory implements ITreeIndexOpHelperFactory {
    
    private static final long serialVersionUID = 1L;

    @Override
    public TreeIndexOpHelper createTreeIndexOpHelper(ITreeIndexOperatorDescriptorHelper opDesc,
            IHyracksStageletContext ctx, int partition, IndexHelperOpenMode mode) {        
        return new BTreeOpHelper(opDesc, ctx, partition, mode);
    }
    
    public ITreeIndexCursor createDiskOrderScanCursor(ITreeIndexFrame leafFrame) throws HyracksDataException {
        return new BTreeDiskOrderScanCursor((IBTreeLeafFrame)leafFrame);
    }
}
