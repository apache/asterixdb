package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

public enum ImmediateScheduler implements ILSMIOScheduler {
    INSTANCE;

    @Override
    public void scheduleFlush(final ILSMIndex index) {
        try {
            ((ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE))
                    .flush();
        } catch (HyracksDataException e) {
            e.printStackTrace();
        } catch (IndexException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void scheduleMerge(final ILSMIndex index) {
        try {
            ((ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE))
                    .merge();
        } catch (LSMMergeInProgressException e) {
            // Ignore!
        } catch (HyracksDataException e) {
            e.printStackTrace();
        } catch (IndexException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void shutdown() {
    }

}
