package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMOperationType;

public interface ILSMComponent {
    public boolean threadEnter(LSMOperationType opType) throws InterruptedException;

    public void threadExit(LSMOperationType opType, boolean failedOperation) throws HyracksDataException;
}
