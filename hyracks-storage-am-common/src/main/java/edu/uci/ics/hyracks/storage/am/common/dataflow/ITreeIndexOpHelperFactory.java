package edu.uci.ics.hyracks.storage.am.common.dataflow;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public interface ITreeIndexOpHelperFactory extends Serializable {
    public TreeIndexOpHelper createTreeIndexOpHelper(ITreeIndexOperatorDescriptorHelper opDesc,
            final IHyracksTaskContext ctx, int partition, IndexHelperOpenMode mode);
}
