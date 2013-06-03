package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;

public class ThreadCountingTracker implements ILSMOperationTracker {
    private final AtomicInteger threadRefCount;

    public ThreadCountingTracker() {
        this.threadRefCount = new AtomicInteger();
    }

    @Override
    public void beforeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        if (opType == LSMOperationType.MODIFICATION) {
            threadRefCount.incrementAndGet();
        }
    }

    @Override
    public void afterOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        // The operation is considered inactive, immediately after leaving the index.
        completeOperation(index, opType, searchCallback, modificationCallback);
    }

    @Override
    public void completeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        // Flush will only be handled by last exiting thread.
        if (opType == LSMOperationType.MODIFICATION) {
            if (threadRefCount.decrementAndGet() == 0 && index.getFlushStatus()) {
                ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                        NoOpOperationCallback.INSTANCE);
                accessor.scheduleFlush(NoOpIOOperationCallback.INSTANCE);
            }
        }
    }
}
