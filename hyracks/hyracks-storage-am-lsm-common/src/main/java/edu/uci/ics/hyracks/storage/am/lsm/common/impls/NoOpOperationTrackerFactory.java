package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;

/**
 * Operation tracker that does nothing.
 * WARNING: This op tracker should only be used for specific testing purposes.
 * It is assumed than an op tracker cooperates with an lsm index to synchronize flushes with
 * regular operations, and this implementation does no such tracking at all.
 */
public class NoOpOperationTrackerFactory implements ILSMOperationTrackerProvider {
    private static final long serialVersionUID = 1L;

    public static NoOpOperationTrackerFactory INSTANCE = new NoOpOperationTrackerFactory();

    @Override
    public ILSMOperationTracker createOperationTracker(IHyracksTaskContext ctx) {
        return new ILSMOperationTracker() {

            @Override
            public void completeOperation(ILSMIndex index, LSMOperationType opType,
                    ISearchOperationCallback searchCallback, IModificationOperationCallback modificationCallback)
                    throws HyracksDataException {
                // Do nothing.
            }

            @Override
            public void beforeOperation(ILSMIndex index, LSMOperationType opType,
                    ISearchOperationCallback searchCallback, IModificationOperationCallback modificationCallback)
                    throws HyracksDataException {
            }

            @Override
            public void afterOperation(ILSMIndex index, LSMOperationType opType,
                    ISearchOperationCallback searchCallback, IModificationOperationCallback modificationCallback)
                    throws HyracksDataException {
                // Do nothing.                        
            }
        };
    }

    // Enforce singleton.
    private NoOpOperationTrackerFactory() {
    }

};
