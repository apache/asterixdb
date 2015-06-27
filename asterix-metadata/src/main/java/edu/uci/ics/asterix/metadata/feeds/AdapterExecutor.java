package edu.uci.ics.asterix.metadata.feeds;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter;
import edu.uci.ics.asterix.common.feeds.api.IAdapterRuntimeManager;
import edu.uci.ics.asterix.common.feeds.api.IAdapterRuntimeManager.State;
import edu.uci.ics.asterix.common.feeds.api.IFeedAdapter;

public class AdapterExecutor implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(AdapterExecutor.class.getName());

    private final DistributeFeedFrameWriter writer;

    private final IFeedAdapter adapter;

    private final IAdapterRuntimeManager adapterManager;

    public AdapterExecutor(int partition, DistributeFeedFrameWriter writer, IFeedAdapter adapter,
            IAdapterRuntimeManager adapterManager) {
        this.writer = writer;
        this.adapter = adapter;
        this.adapterManager = adapterManager;
    }

    @Override
    public void run() {
        int partition = adapterManager.getPartition();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting ingestion for partition:" + partition);
        }
        boolean continueIngestion = true;
        boolean failedIngestion = false;
        while (continueIngestion) {
            try {
                adapter.start(partition, writer);
                continueIngestion = false;
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe("Exception during feed ingestion " + e.getMessage());
                    e.printStackTrace();
                }
                continueIngestion = adapter.handleException(e);
                failedIngestion = !continueIngestion;
            }
        }

        adapterManager.setState(failedIngestion ? State.FAILED_INGESTION : State.FINISHED_INGESTION);
        synchronized (adapterManager) {
            adapterManager.notifyAll();
        }
    }

}