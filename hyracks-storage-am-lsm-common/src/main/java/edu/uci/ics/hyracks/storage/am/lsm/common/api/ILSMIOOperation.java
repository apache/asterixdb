package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import java.util.Set;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;

public interface ILSMIOOperation {
    public Set<IODeviceHandle> getReadDevices();

    public Set<IODeviceHandle> getWriteDevices();

    public void perform() throws HyracksDataException, IndexException;

    public ILSMIOOperationCallback getCallback();
}
