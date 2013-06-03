package edu.uci.ics.asterix.transaction.management.opcallbacks;

import edu.uci.ics.asterix.common.context.BaseOperationTracker;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;

public class SecondaryIndexOperationTrackerFactory implements ILSMOperationTrackerProvider {

    private static final long serialVersionUID = 1L;

    private final ILSMIOOperationCallbackFactory ioOpCallbackFactory;

    public SecondaryIndexOperationTrackerFactory(ILSMIOOperationCallbackFactory ioOpCallbackFactory) {
        this.ioOpCallbackFactory = ioOpCallbackFactory;
    }

    @Override
    public ILSMOperationTracker createOperationTracker(IHyracksTaskContext ctx) {
        return new BaseOperationTracker(ioOpCallbackFactory);
    }

}
