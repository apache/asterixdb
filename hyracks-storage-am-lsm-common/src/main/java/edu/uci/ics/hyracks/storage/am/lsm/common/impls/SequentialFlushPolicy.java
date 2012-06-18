package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushPolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

public enum SequentialFlushPolicy implements ILSMFlushPolicy {
    INSTANCE;

    private static final Logger LOGGER = Logger.getLogger(SequentialFlushPolicy.class.getName());

    private final ExecutorService executor;

    private SequentialFlushPolicy() {
        executor = Executors.newSingleThreadExecutor();
    }

    @Override
    public void memoryComponentFull(final ILSMIndex index) {
        executor.submit(new Runnable() {
            public void run() {
                try {
                    LOGGER.info("flushing");
                    ((ILSMIndexAccessor) index.createAccessor()).flush();
                    LOGGER.info("finished flushing");
                } catch (HyracksDataException e) {
                    LOGGER.info(e.getMessage());
                } catch (IndexException e) {
                    LOGGER.info(e.getMessage());
                }
                LOGGER.info("Thread should end");
            }
        });
    }
}
