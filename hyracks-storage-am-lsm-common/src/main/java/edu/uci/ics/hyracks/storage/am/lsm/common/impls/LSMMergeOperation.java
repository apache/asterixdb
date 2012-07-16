package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

public class LSMMergeOperation implements ILSMIOOperation {

    private final ILSMIndex index;

    public LSMMergeOperation(ILSMIndex index) {
        this.index = index;
    }

    @Override
    public List<IODeviceHandle> getReadDevices() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<IODeviceHandle> getWriteDevices() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void perform() throws HyracksDataException, IndexException {
        ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        accessor.merge();
    }

    @Override
    public ILSMIOOperationCallback getCallback() {
        return NoOpIOOperationCallback.INSTANCE;
    }

}
