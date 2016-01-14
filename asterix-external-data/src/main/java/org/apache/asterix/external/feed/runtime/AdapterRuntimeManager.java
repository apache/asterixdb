/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.feed.runtime;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.external.api.IAdapterRuntimeManager;
import org.apache.asterix.external.api.IFeedAdapter;
import org.apache.asterix.external.feed.api.IIntakeProgressTracker;
import org.apache.asterix.external.feed.dataflow.DistributeFeedFrameWriter;
import org.apache.asterix.external.feed.management.FeedId;
import org.apache.log4j.Logger;

/**
 * This class manages the execution of an adapter within a feed
 */
public class AdapterRuntimeManager implements IAdapterRuntimeManager {

    private static final Logger LOGGER = Logger.getLogger(AdapterRuntimeManager.class.getName());

    private final FeedId feedId;                    // (dataverse-feed)

    private final IFeedAdapter feedAdapter;         // The adapter

    private final IIntakeProgressTracker tracker;   // Not used. needs to be fixed soon.

    private final AdapterExecutor adapterExecutor;  // The executor for the adapter <-- two way visibility -->

    private final int partition;                    // The partition number

    private final ExecutorService executorService;  // Executor service to run/shutdown the adapter executor

    private IngestionRuntime ingestionRuntime;      // Runtime representing the ingestion stage of a feed <-- two way
                                                    // visibility -->

    private State state;                            // One of {ACTIVE_INGESTION, NACTIVE_INGESTION, FINISHED_INGESTION,
                                                    // FAILED_INGESTION}

    public AdapterRuntimeManager(FeedId feedId, IFeedAdapter feedAdapter, IIntakeProgressTracker tracker,
            DistributeFeedFrameWriter writer, int partition) {
        this.feedId = feedId;
        this.feedAdapter = feedAdapter;
        this.tracker = tracker;
        this.partition = partition;
        this.adapterExecutor = new AdapterExecutor(partition, writer, feedAdapter, this);
        this.executorService = Executors.newSingleThreadExecutor();
        this.state = State.INACTIVE_INGESTION;
    }

    @Override
    public void start() throws Exception {
        state = State.ACTIVE_INGESTION;
        executorService.execute(adapterExecutor);
    }

    @Override
    public void stop() {
        boolean stopped = false;
        try {
            stopped = feedAdapter.stop();
        } catch (Exception exception) {
            LOGGER.error("Unable to stop adapter " + feedAdapter, exception);
        } finally {
            state = State.FINISHED_INGESTION;
            if (stopped) {
                // stop() returned true, we wait for the process termination
                executorService.shutdown();
                try {
                    executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    LOGGER.error("Interrupted while waiting for feed adapter to finish its work", e);
                }
            } else {
                // stop() returned false, we try to force shutdown
                executorService.shutdownNow();
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

    @Override
    public IFeedAdapter getFeedAdapter() {
        return feedAdapter;
    }

    public IIntakeProgressTracker getTracker() {
        return tracker;
    }

    @Override
    public synchronized State getState() {
        return state;
    }

    @Override
    public synchronized void setState(State state) {
        this.state = state;
    }

    public AdapterExecutor getAdapterExecutor() {
        return adapterExecutor;
    }

    @Override
    public int getPartition() {
        return partition;
    }

    public IngestionRuntime getIngestionRuntime() {
        return ingestionRuntime;
    }

    @Override
    public IIntakeProgressTracker getProgressTracker() {
        return tracker;
    }

}
