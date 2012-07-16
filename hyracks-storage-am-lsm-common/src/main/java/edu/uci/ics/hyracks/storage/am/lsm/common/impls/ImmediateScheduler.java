package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;

public enum ImmediateScheduler implements ILSMIOOperationScheduler {
    INSTANCE;

    @Override
    public void scheduleOperation(ILSMIOOperation operation) {
        try {
            operation.perform();
        } catch (HyracksDataException e) {
            e.printStackTrace();
        } catch (IndexException e) {
            e.printStackTrace();
        } finally {
            operation.getCallback().callback();
        }
    }

}
