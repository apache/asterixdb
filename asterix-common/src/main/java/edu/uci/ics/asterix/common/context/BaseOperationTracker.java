package edu.uci.ics.asterix.common.context;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMOperationType;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;

public class BaseOperationTracker implements ILSMOperationTracker {

    protected final ILSMIOOperationCallback ioOpCallback;
    protected long lastLSN;
    protected long firstLSN;

    public BaseOperationTracker(ILSMIOOperationCallbackFactory ioOpCallbackFactory) {
        this.ioOpCallback = ioOpCallbackFactory == null ? NoOpIOOperationCallback.INSTANCE : ioOpCallbackFactory
                .createIOOperationCallback(this);
        resetLSNs();
    }

    public ILSMIOOperationCallback getIOOperationCallback() {
        return ioOpCallback;
    }

    public long getLastLSN() {
        return lastLSN;
    }

    public long getFirstLSN() {
        return firstLSN;
    }

    public void updateLastLSN(long lastLSN) {
        if (firstLSN == -1) {
            firstLSN = lastLSN;
        }
        this.lastLSN = Math.max(this.lastLSN, lastLSN);
    }

    public void resetLSNs() {
        lastLSN = -1;
        firstLSN = -1;
    }

    @Override
    public void beforeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
    }

    @Override
    public void afterOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
    }

    @Override
    public void completeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
    }

}
