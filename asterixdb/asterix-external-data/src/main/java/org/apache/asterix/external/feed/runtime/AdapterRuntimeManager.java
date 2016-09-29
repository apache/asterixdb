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

import java.util.concurrent.Future;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.external.dataset.adapter.FeedAdapter;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
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

    private final IHyracksTaskContext ctx;

    private IngestionRuntime ingestionRuntime; // Runtime representing the ingestion stage of a feed

    private Future<?> execution;

    private volatile boolean done = false;
    private volatile boolean failed = false;

    public AdapterRuntimeManager(IHyracksTaskContext ctx, EntityId entityId, FeedAdapter feedAdapter,
                                 IFrameWriter writer, int partition) {
        this.ctx = ctx;
        this.feedId = entityId;
        this.feedAdapter = feedAdapter;
        this.partition = partition;
        this.adapterExecutor = new AdapterExecutor(writer, feedAdapter, this);
    }

    public void start() {
        execution = ctx.getExecutorService().submit(adapterExecutor);
    }

    public void stop() throws InterruptedException {
        try {
            if (feedAdapter.stop()) {
                // stop() returned true, we wait for the process termination
                execution.get();
            } else {
                // stop() returned false, we try to force shutdown
                execution.cancel(true);
            }
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while waiting for feed adapter to finish its work", e);
            throw e;
        } catch (Exception exception) {
            LOGGER.error("Unable to stop adapter " + feedAdapter, exception);
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
