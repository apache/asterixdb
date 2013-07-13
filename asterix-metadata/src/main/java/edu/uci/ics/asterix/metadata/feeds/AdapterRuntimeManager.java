/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.metadata.feeds;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.AsterixThreadExecutor;
import edu.uci.ics.asterix.metadata.feeds.MaterializingFrameWriter.Mode;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class AdapterRuntimeManager implements IAdapterExecutor {

    private static final Logger LOGGER = Logger.getLogger(AdapterRuntimeManager.class.getName());

    private final FeedId feedId;

    private IFeedAdapter feedAdapter;

    private AdapterExecutor adapterExecutor;

    private Thread adapterRuntime;

    private State state;

    private FeedInboxMonitor feedInboxMonitor;

    private int partition;

    public enum State {
        /*
         * Indicates that data from external source will be pushed downstream for storage 
         */
        ACTIVE_INGESTION,
        /*
         * Indicates that data from external source would be buffered and not pushed downstream 
         */
        INACTIVE_INGESTION,
        /*
         * Indicates that feed ingestion activity has finished
         */
        FINISHED_INGESTION
    }

    public AdapterRuntimeManager(FeedId feedId, IFeedAdapter feedAdapter, MaterializingFrameWriter writer,
            int partition, LinkedBlockingQueue<IFeedMessage> inbox) {
        this.feedId = feedId;
        this.feedAdapter = feedAdapter;
        this.partition = partition;
        this.adapterExecutor = new AdapterExecutor(partition, writer, feedAdapter, this);
        this.adapterRuntime = new Thread(adapterExecutor);
        this.feedInboxMonitor = new FeedInboxMonitor(this, inbox, partition);
        AsterixThreadExecutor.INSTANCE.execute(feedInboxMonitor);
    }

    @Override
    public void start() throws Exception {
        state = State.ACTIVE_INGESTION;
        FeedManager.INSTANCE.registerFeedRuntime(this);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Registered feed runtime manager for " + this.getFeedId());
        }
        adapterRuntime.start();
    }

    @Override
    public void stop() {
        try {
            feedAdapter.stop();
            state = State.FINISHED_INGESTION;
            synchronized (this) {
                notifyAll();
            }
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to stop adapter");
            }
        }
    }

    @Override
    public FeedId getFeedId() {
        return feedId;
    }

    @Override
    public String toString() {
        return feedId + "[" + partition + "]";
    }

    public IFeedAdapter getFeedAdapter() {
        return feedAdapter;
    }

    public void setFeedAdapter(IFeedAdapter feedAdapter) {
        this.feedAdapter = feedAdapter;
    }

    public static class AdapterExecutor implements Runnable {

        private MaterializingFrameWriter writer;

        private IFeedAdapter adapter;

        private AdapterRuntimeManager runtimeManager;

        public AdapterExecutor(int partition, MaterializingFrameWriter writer, IFeedAdapter adapter,
                AdapterRuntimeManager adapterRuntimeMgr) {
            this.writer = writer;
            this.adapter = adapter;
            this.runtimeManager = adapterRuntimeMgr;
        }

        @Override
        public void run() {
            try {
                int partition = runtimeManager.getPartition();
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Starting ingestion for partition:" + partition);
                }
                adapter.start(partition, writer);
                runtimeManager.setState(State.FINISHED_INGESTION);
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe("Exception during feed ingestion " + e.getMessage());
                }
                e.printStackTrace();
            } finally {
                synchronized (runtimeManager) {
                    runtimeManager.notifyAll();
                }
            }
        }

        public MaterializingFrameWriter getWriter() {
            return writer;
        }

        public void setWriter(IFrameWriter writer) {
            if (this.writer != null) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Switching writer to:" + writer + " from " + this.writer);
                }
                this.writer.setWriter(writer);
            }
        }

    }

    public synchronized State getState() {
        return state;
    }

    @SuppressWarnings("incomplete-switch")
    public synchronized void setState(State state) throws HyracksDataException {
        if (this.state.equals(state)) {
            return;
        }
        switch (state) {
            case INACTIVE_INGESTION:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Set " + Mode.STORE + " mode");
                }
                adapterExecutor.getWriter().setMode(Mode.STORE);
                break;
            case ACTIVE_INGESTION:
                adapterExecutor.getWriter().setMode(Mode.FORWARD);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Set " + Mode.FORWARD + " mode");
                }
                break;
        }
        this.state = state;
    }

    public AdapterExecutor getAdapterExecutor() {
        return adapterExecutor;
    }

    public int getPartition() {
        return partition;
    }

}
