package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOSchedulerProvider;

public enum ImmediateSchedulerProvider implements ILSMIOSchedulerProvider {
    INSTANCE;

    @Override
    public ILSMIOOperationScheduler getIOScheduler() {
        return ImmediateScheduler.INSTANCE;
    }

}
