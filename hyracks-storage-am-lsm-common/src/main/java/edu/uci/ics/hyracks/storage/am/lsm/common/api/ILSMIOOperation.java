package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;

public interface ILSMIOOperation {
    public List<IODeviceHandle> getReadDevices();

    public List<IODeviceHandle> getWriteDevices();

    public void perform() throws HyracksDataException, IndexException;

    public ILSMIOOperationCallback getCallback();
}
