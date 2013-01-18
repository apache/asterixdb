package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;

public class ThreadCountingOperationTrackerFactory implements ILSMOperationTrackerFactory {

    private static final long serialVersionUID = 1L;
    
    public static ThreadCountingOperationTrackerFactory INSTANCE = new ThreadCountingOperationTrackerFactory(); 
    
    @Override
    public ILSMOperationTracker createOperationTracker(ILSMIndex index) {
        return new ThreadCountingTracker(index);
    }

    // Enforce singleton.
    private ThreadCountingOperationTrackerFactory() {
    }
}
