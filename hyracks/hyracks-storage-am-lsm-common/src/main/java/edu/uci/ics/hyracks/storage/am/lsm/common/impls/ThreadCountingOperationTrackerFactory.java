package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;

public class ThreadCountingOperationTrackerFactory implements ILSMOperationTrackerProvider {

    private static final long serialVersionUID = 1L;

    public static ThreadCountingOperationTrackerFactory INSTANCE = new ThreadCountingOperationTrackerFactory();

    @Override
    public ILSMOperationTracker createOperationTracker(IHyracksTaskContext ctx) {
        return new ThreadCountingTracker();
    }

    // Enforce singleton.
    private ThreadCountingOperationTrackerFactory() {
    }
}
