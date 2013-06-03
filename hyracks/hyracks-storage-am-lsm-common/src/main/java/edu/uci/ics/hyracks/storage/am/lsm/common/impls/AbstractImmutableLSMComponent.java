package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;

public abstract class AbstractImmutableLSMComponent implements ILSMComponent {

    private ComponentState state;
    private int readerCount;

    private enum ComponentState {
        READABLE,
        READABLE_MERGING,
        KILLED
    }

    public AbstractImmutableLSMComponent() {
        state = ComponentState.READABLE;
        readerCount = 0;
    }

    @Override
    public synchronized boolean threadEnter(LSMOperationType opType) {
        if (state == ComponentState.KILLED) {
            return false;
        }

        switch (opType) {
            case FORCE_MODIFICATION:
            case MODIFICATION:
            case SEARCH:
                readerCount++;
                break;
            case MERGE:
                if (state == ComponentState.READABLE_MERGING) {
                    return false;
                }
                state = ComponentState.READABLE_MERGING;
                readerCount++;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation " + opType);
        }
        return true;
    }

    @Override
    public synchronized void threadExit(LSMOperationType opType, boolean failedOperation) throws HyracksDataException {
        switch (opType) {
            case MERGE:
                if (failedOperation) {
                    state = ComponentState.READABLE;
                }
            case FORCE_MODIFICATION:
            case MODIFICATION:
            case SEARCH:
                readerCount--;

                if (readerCount == 0 && state == ComponentState.READABLE_MERGING) {
                    destroy();
                    state = ComponentState.KILLED;
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation " + opType);
        }
    }

    protected abstract void destroy() throws HyracksDataException;

}
