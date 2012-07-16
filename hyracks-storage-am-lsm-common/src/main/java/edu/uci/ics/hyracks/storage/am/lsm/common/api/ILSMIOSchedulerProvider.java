package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import java.io.Serializable;

public interface ILSMIOSchedulerProvider extends Serializable {
    public ILSMIOOperationScheduler getIOScheduler();
}
