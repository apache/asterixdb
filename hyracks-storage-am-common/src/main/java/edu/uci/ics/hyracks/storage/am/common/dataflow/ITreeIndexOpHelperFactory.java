package edu.uci.ics.hyracks.storage.am.common.dataflow;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;

public interface ITreeIndexOpHelperFactory extends Serializable {
	public TreeIndexOpHelper createTreeIndexOpHelper(ITreeIndexOperatorDescriptorHelper opDesc, final IHyracksStageletContext ctx, int partition,
            IndexHelperOpenMode mode);
}
