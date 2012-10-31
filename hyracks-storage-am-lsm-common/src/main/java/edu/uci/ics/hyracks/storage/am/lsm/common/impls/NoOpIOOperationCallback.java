package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;

public enum NoOpIOOperationCallback implements ILSMIOOperationCallback {
    INSTANCE;

    @Override
    public void beforeOperation(ILSMIOOperation operation) throws HyracksDataException {
        // Do nothing.
    }

    @Override
    public void afterOperation(ILSMIOOperation operation, Object newComponent) throws HyracksDataException {
        // Do nothing.
    }

    @Override
    public void afterFinalize(ILSMIOOperation operation, Object newComponent) throws HyracksDataException {
        // Do nothing.
    }
}
