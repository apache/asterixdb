package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;

public enum NoMergePolicy implements ILSMMergePolicy {
    INSTANCE;

    @Override
    public void componentAdded(ILSMIndex index, int totalNumDiskComponents, boolean mergeInProgress) {
        // Do nothing
    }

}
