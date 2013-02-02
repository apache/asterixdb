package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyProvider;

public class ConstantMergePolicyProvider implements ILSMMergePolicyProvider {

    private static final long serialVersionUID = 1L;

    private final int threshold;

    public ConstantMergePolicyProvider(int threshold) {
        this.threshold = threshold;
    }

    @Override
    public ILSMMergePolicy getMergePolicy(IHyracksTaskContext ctx) {
        return new ConstantMergePolicy(threshold);
    }

}
