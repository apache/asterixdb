package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;

/**
 * Operation tracker that does nothing.
 * WARNING: This op tracker should only be used for specific testing purposes.
 * It is assumed than an op tracker cooperates with an lsm index to synchronize flushes with
 * regular operations, and this implementation does no such tracking at all.
 */
public class NoOpOperationTrackerFactory implements ILSMOperationTrackerFactory {
    private static final long serialVersionUID = 1L;

    public static NoOpOperationTrackerFactory INSTANCE = new NoOpOperationTrackerFactory();

    @Override
    public ILSMOperationTracker createOperationTracker(ILSMIndex index) {
        return new ILSMOperationTracker() {

            @Override
            public void completeOperation(ILSMIndexOperationContext opCtx) throws HyracksDataException {
                // Do nothing.
            }

            @Override
            public boolean beforeOperation(ILSMIndexOperationContext opCtx, boolean tryOperation)
                    throws HyracksDataException {
                // Do nothing.
                return true;
            }

            @Override
            public void afterOperation(ILSMIndexOperationContext opCtx) throws HyracksDataException {
                // Do nothing.                        
            }
        };
    }

    // Enforce singleton.
    private NoOpOperationTrackerFactory() {
    }

};
