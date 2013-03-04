package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface ILSMIOOperationCallback {
    public void beforeOperation() throws HyracksDataException;

    public void afterOperation(List<ILSMComponent> oldComponents, ILSMComponent newComponent)
            throws HyracksDataException;

    public void afterFinalize(ILSMComponent newComponent) throws HyracksDataException;
}
