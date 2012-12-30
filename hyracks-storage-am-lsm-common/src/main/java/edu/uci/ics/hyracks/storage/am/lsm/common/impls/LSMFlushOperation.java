package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.util.Collections;
import java.util.Set;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;

public class LSMFlushOperation implements ILSMIOOperation {

    private final ILSMIndexAccessorInternal accessor;
    private final FileReference flushTarget;
    private final ILSMIOOperationCallback callback;

    public LSMFlushOperation(ILSMIndexAccessorInternal accessor, FileReference flushTarget,
            ILSMIOOperationCallback callback) {
        this.accessor = accessor;
        this.flushTarget = flushTarget;
        this.callback = callback;
    }

    @Override
    public Set<IODeviceHandle> getReadDevices() {
        return Collections.emptySet();
    }

    @Override
    public Set<IODeviceHandle> getWriteDevices() {
        return Collections.singleton(flushTarget.getDevideHandle());
    }

    @Override
    public void perform() throws HyracksDataException, IndexException {
        accessor.flush(this);
    }

    @Override
    public ILSMIOOperationCallback getCallback() {
        return callback;
    }

    public FileReference getFlushTarget() {
        return flushTarget;
    }

}
