package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;

public enum NoOpIOOperationCallback implements ILSMIOOperationCallback, ILSMIOOperationCallbackProvider {
    INSTANCE;

    @Override
    public void beforeOperation() throws HyracksDataException {
        // Do nothing.
    }

    @Override
    public void afterOperation(List<ILSMComponent> oldComponents, ILSMComponent newComponent)
            throws HyracksDataException {
        // Do nothing.
    }

    @Override
    public void afterFinalize(ILSMComponent newComponent) throws HyracksDataException {
        // Do nothing.
    }

    @Override
    public ILSMIOOperationCallback getIOOperationCallback(ILSMIndex index) {
        return INSTANCE;
    }
}
