package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;

public class ConstantMergePolicy implements ILSMMergePolicy {

    private final ILSMIOOperationScheduler ioScheduler;

    private final int threshold;

    public ConstantMergePolicy(ILSMIOOperationScheduler ioScheduler, int threshold) {
        this.ioScheduler = ioScheduler;
        this.threshold = threshold;
    }

    @Override
    public void diskComponentAdded(final ILSMIndex index, int totalNumDiskComponents) throws HyracksDataException {
        if (totalNumDiskComponents >= threshold) {
            ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            ILSMIOOperation op;
            try {
                op = accessor.createMergeOperation(NoOpIOOperationCallback.INSTANCE);
                if (op != null) {
                    ioScheduler.scheduleOperation(op);
                }
            } catch (LSMMergeInProgressException e) {
                // Do nothing
            }
        }
    }
}
