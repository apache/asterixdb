package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.common.dataflow.ITreeIndexOpHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.ITreeIndexOperatorDescriptorHelper;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexHelperOpenMode;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexOpHelper;

public class BTreeOpHelperFactory implements ITreeIndexOpHelperFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public TreeIndexOpHelper createTreeIndexOpHelper(ITreeIndexOperatorDescriptorHelper opDesc,
            IHyracksTaskContext ctx, int partition, IndexHelperOpenMode mode) {
        return new BTreeOpHelper(opDesc, ctx, partition, mode);
    }

}
