package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

public enum SequentialScheduler implements ILSMIOScheduler {
    INSTANCE;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Override
    public void scheduleFlush(final ILSMIndex index) {
        executor.submit(new Runnable() {

            @Override
            public void run() {
                try {
                    ((ILSMIndexAccessor) index.createAccessor()).flush();
                } catch (HyracksDataException e) {
                    e.printStackTrace();
                } catch (IndexException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void scheduleMerge(final ILSMIndex index) {
        executor.submit(new Runnable() {

            @Override
            public void run() {
                try {
                    ((ILSMIndexAccessor) index.createAccessor()).merge();
                } catch (LSMMergeInProgressException e) {
                    // Ignore!
                } catch (HyracksDataException e) {
                    e.printStackTrace();
                } catch (IndexException e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
