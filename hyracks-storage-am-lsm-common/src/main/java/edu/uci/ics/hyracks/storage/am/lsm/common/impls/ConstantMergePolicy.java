package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;

public class ConstantMergePolicy implements ILSMMergePolicy {

    private final ILSMIOScheduler ioScheduler;

    private final int threshold;

    public ConstantMergePolicy(ILSMIOScheduler ioScheduler, int threshold) {
        this.ioScheduler = ioScheduler;
        this.threshold = threshold;
    }

    @Override
    public void diskComponentAdded(final ILSMIndex index, int totalNumDiskComponents) {
        if (totalNumDiskComponents >= threshold) {
            ioScheduler.scheduleMerge(index);
        }
    }
}
