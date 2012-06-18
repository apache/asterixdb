package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOSchedulerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyProvider;

public class ConstantMergePolicyProvider implements ILSMMergePolicyProvider {

    private static final long serialVersionUID = 1L;

    private final ILSMIOSchedulerProvider schedulerProvider;

    private final int threshold;

    public ConstantMergePolicyProvider(ILSMIOSchedulerProvider schedulerProvider, int threshold) {
        this.schedulerProvider = schedulerProvider;
        this.threshold = threshold;
    }

    @Override
    public ILSMMergePolicy getMergePolicy() {
        return new ConstantMergePolicy(schedulerProvider.getIOScheduler(), threshold);
    }

}
