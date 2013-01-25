package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;

public class LSMRTreeFlushOperation implements ILSMIOOperation {

    private final ILSMIndexAccessorInternal accessor;
    private final ILSMComponent flushingComponent;
    private final FileReference rtreeFlushTarget;
    private final FileReference btreeFlushTarget;
    private final ILSMIOOperationCallback callback;

    public LSMRTreeFlushOperation(ILSMIndexAccessorInternal accessor, ILSMComponent flushingComponent,
            FileReference rtreeFlushTarget, FileReference btreeFlushTarget, ILSMIOOperationCallback callback) {
        this.accessor = accessor;
        this.flushingComponent = flushingComponent;
        this.rtreeFlushTarget = rtreeFlushTarget;
        this.btreeFlushTarget = btreeFlushTarget;
        this.callback = callback;
    }

    @Override
    public Set<IODeviceHandle> getReadDevices() {
        return Collections.emptySet();
    }

    @Override
    public Set<IODeviceHandle> getWriteDevices() {
        Set<IODeviceHandle> devs = new HashSet<IODeviceHandle>();
        devs.add(rtreeFlushTarget.getDeviceHandle());
        if (btreeFlushTarget != null) {
            devs.add(btreeFlushTarget.getDeviceHandle());
        }
        return devs;
    }

    @Override
    public void perform() throws HyracksDataException, IndexException {
        accessor.flush(this);
    }

    @Override
    public ILSMIOOperationCallback getCallback() {
        return callback;
    }

    public FileReference getRTreeFlushTarget() {
        return rtreeFlushTarget;
    }

    public FileReference getBTreeFlushTarget() {
        return btreeFlushTarget;
    }

    public ILSMComponent getFlushingComponent() {
        return flushingComponent;
    }
}
