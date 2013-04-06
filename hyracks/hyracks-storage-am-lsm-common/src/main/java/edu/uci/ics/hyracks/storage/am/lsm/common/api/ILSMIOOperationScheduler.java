package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface ILSMIOOperationScheduler {
    public void scheduleOperation(ILSMIOOperation operation) throws HyracksDataException;
}
