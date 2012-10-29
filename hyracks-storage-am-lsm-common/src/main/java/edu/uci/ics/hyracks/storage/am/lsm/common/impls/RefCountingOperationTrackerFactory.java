package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;

public class RefCountingOperationTrackerFactory implements ILSMOperationTrackerFactory {

    private static final long serialVersionUID = 1L;
    
    public static RefCountingOperationTrackerFactory INSTANCE = new RefCountingOperationTrackerFactory(); 
    
    @Override
    public ILSMOperationTracker createOperationTracker(ILSMIndex index) {
        return new ReferenceCountingOperationTracker(index);
    }

    // Enforce singleton.
    private RefCountingOperationTrackerFactory() {
    }
}
