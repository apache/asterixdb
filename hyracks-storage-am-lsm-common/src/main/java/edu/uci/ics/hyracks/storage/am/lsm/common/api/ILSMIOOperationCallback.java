package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface ILSMIOOperationCallback {
    public void beforeOperation(ILSMIOOperation operation) throws HyracksDataException;

    public void afterOperation(ILSMIOOperation operation, List<Object> oldComponents, Object newComponent)
            throws HyracksDataException;

    public void afterFinalize(ILSMIOOperation operation, Object newComponent) throws HyracksDataException;
}
