package edu.uci.ics.asterix.transaction.management.opcallbacks;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;

public class SecondaryIndexOperationTrackerFactory implements ILSMOperationTrackerFactory {

    private static final long serialVersionUID = 1L;

    private final ILSMIOOperationCallbackFactory ioOpCallbackFactory;

    public SecondaryIndexOperationTrackerFactory(ILSMIOOperationCallbackFactory ioOpCallbackFactory) {
        this.ioOpCallbackFactory = ioOpCallbackFactory;
    }

    @Override
    public ILSMOperationTracker createOperationTracker(ILSMIndex index) {
        return new BaseOperationTracker(index, ioOpCallbackFactory);
    }

}
