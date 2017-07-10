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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.external.dataset.adapter.FeedAdapter;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

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

    private Future<?> execution;

    private boolean started = false;
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
        synchronized (adapterExecutor) {
            started = true;
            if (!done) {
                execution = ctx.getExecutorService().submit(adapterExecutor);
            } else {
                LOGGER.log(Level.WARNING, "Someone stopped me before I even start. I will simply not start");
            }
        }
    }

    public void stop() throws HyracksDataException, InterruptedException {
        synchronized (adapterExecutor) {
            try {
                if (started) {
                    try {
                        ctx.getExecutorService().submit(() -> {
                            if (feedAdapter.stop()) {
                                execution.get();
                            }
                            return null;
                        }).get(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        LOGGER.log(Level.WARNING, "Interrupted while trying to stop an adapter runtime", e);
                        throw e;
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "Exception while trying to stop an adapter runtime", e);
                        throw HyracksDataException.create(e);
                    } finally {
                        execution.cancel(true);
                    }
                } else {
                    LOGGER.log(Level.WARNING, "Adapter executor was stopped before it starts");
                }
            } finally {
                done = true;
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

    public String getStats() {
        return adapterExecutor.getStats();
    }
}
