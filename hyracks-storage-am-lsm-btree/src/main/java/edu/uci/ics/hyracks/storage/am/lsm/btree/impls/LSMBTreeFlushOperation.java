package edu.uci.ics.hyracks.storage.am.lsm.btree.impls;

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

public class LSMBTreeFlushOperation implements ILSMIOOperation {

    private final ILSMIndexAccessorInternal accessor;
    private final ILSMComponent flushingComponent;
    private final FileReference btreeFlushTarget;
    private final FileReference bloomFilterFlushTarget;
    private final ILSMIOOperationCallback callback;

    public LSMBTreeFlushOperation(ILSMIndexAccessorInternal accessor, ILSMComponent flushingComponent,
            FileReference btreeFlushTarget, FileReference bloomFilterFlushTarget, ILSMIOOperationCallback callback) {
        this.accessor = accessor;
        this.flushingComponent = flushingComponent;
        this.btreeFlushTarget = btreeFlushTarget;
        this.bloomFilterFlushTarget = bloomFilterFlushTarget;
        this.callback = callback;
    }

    @Override
    public Set<IODeviceHandle> getReadDevices() {
        return Collections.emptySet();
    }

    @Override
    public Set<IODeviceHandle> getWriteDevices() {
        Set<IODeviceHandle> devs = new HashSet<IODeviceHandle>();
        devs.add(btreeFlushTarget.getDeviceHandle());
        devs.add(bloomFilterFlushTarget.getDeviceHandle());
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

    public FileReference getBTreeFlushTarget() {
        return btreeFlushTarget;
    }

    public FileReference getBloomFilterFlushTarget() {
        return bloomFilterFlushTarget;
    }

    public ILSMIndexAccessorInternal getAccessor() {
        return accessor;
    }

    public ILSMComponent getFlushingComponent() {
        return flushingComponent;
    }
}
