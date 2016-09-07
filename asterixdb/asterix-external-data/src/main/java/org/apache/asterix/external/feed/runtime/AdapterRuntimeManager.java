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

import org.apache.asterix.active.EntityId;
import org.apache.asterix.external.dataset.adapter.FeedAdapter;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.log4j.Logger;

/**
 * This class manages the execution of an adapter within a feed
 */
public class AdapterRuntimeManager {

    private static final Logger LOGGER = Logger.getLogger(AdapterRuntimeManager.class.getName());

    private final EntityId feedId; // (dataverse-feed)

    private final FeedAdapter feedAdapter; // The adapter

    private final AdapterExecutor adapterExecutor; // The executor for the adapter

    private final int partition; // The partition number

    private final ExecutorService executorService; // Executor service to run/shutdown the adapter executor

    private IngestionRuntime ingestionRuntime; // Runtime representing the ingestion stage of a feed

    private volatile boolean done = false;
    private volatile boolean failed = false;

    public AdapterRuntimeManager(EntityId entityId, FeedAdapter feedAdapter, IFrameWriter writer, int partition) {
        this.feedId = entityId;
        this.feedAdapter = feedAdapter;
        this.partition = partition;
        this.adapterExecutor = new AdapterExecutor(writer, feedAdapter, this);
        this.executorService = Executors.newSingleThreadExecutor();
    }

    public void start() {
        executorService.execute(adapterExecutor);
    }

    public void stop() throws InterruptedException {
        boolean stopped = false;
        try {
            stopped = feedAdapter.stop();
        } catch (Exception exception) {
            LOGGER.error("Unable to stop adapter " + feedAdapter, exception);
        } finally {
            if (stopped) {
                // stop() returned true, we wait for the process termination
                executorService.shutdown();
                try {
                    executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    LOGGER.error("Interrupted while waiting for feed adapter to finish its work", e);
                    throw e;
                }
            } else {
                // stop() returned false, we try to force shutdown
                executorService.shutdownNow();
            }
        }
    }

    public EntityId getFeedId() {
        return feedId;
    }

    @Override
    public String toString() {
        return feedId + "[" + partition + "]";
    }

    public FeedAdapter getFeedAdapter() {
        return feedAdapter;
    }

    public AdapterExecutor getAdapterExecutor() {
        return adapterExecutor;
    }

    public int getPartition() {
        return partition;
    }

    public IngestionRuntime getIngestionRuntime() {
        return ingestionRuntime;
    }

    public boolean isFailed() {
        return failed;
    }

    public void setFailed(boolean failed) {
        this.failed = failed;
    }

    public boolean isDone() {
        return done;
    }

    public void setDone(boolean done) {
        this.done = done;
    }
}
