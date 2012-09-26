package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;

public class RefCountingOperationTrackerProvider implements ILSMOperationTrackerProvider {

    private static final long serialVersionUID = 1L;

    @Override
    public ILSMOperationTracker getOperationTracker(IHyracksTaskContext ctx) {
        return new ReferenceCountingOperationTracker();
    }

}
