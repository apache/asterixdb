package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;

public class ConstantMergePolicy implements ILSMMergePolicy {

    private final ExecutorService executor;

    private final int threshold;

    public ConstantMergePolicy(int threshold) {
        this.executor = Executors.newCachedThreadPool();
        this.threshold = threshold;
    }

    @Override
    public void componentAdded(final ILSMIndex index, int totalNumDiskComponents, boolean mergeInProgress) {
        synchronized (index) {
            if (totalNumDiskComponents >= threshold && !mergeInProgress) {
                executor.submit(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            ((ILSMIndexAccessor) index.createAccessor()).merge();
                        } catch (HyracksDataException e) {
                            e.printStackTrace();
                        } catch (IndexException e) {
                            e.printStackTrace();
                        }
                    }
                });

            }
        }
    }
}
