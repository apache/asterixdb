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
package org.apache.asterix.metadata.feeds;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.feeds.DistributeFeedFrameWriter;
import org.apache.asterix.common.feeds.FeedId;
import org.apache.asterix.common.feeds.IngestionRuntime;
import org.apache.asterix.common.feeds.api.IAdapterRuntimeManager;
import org.apache.asterix.common.feeds.api.IFeedAdapter;
import org.apache.asterix.common.feeds.api.IIntakeProgressTracker;

public class AdapterRuntimeManager implements IAdapterRuntimeManager {

    private static final Logger LOGGER = Logger.getLogger(AdapterRuntimeManager.class.getName());

    private final FeedId feedId;

    private final IFeedAdapter feedAdapter;

    private final IIntakeProgressTracker tracker;

    private final AdapterExecutor adapterExecutor;

    private final int partition;

    private final ExecutorService executorService;

    private IngestionRuntime ingestionRuntime;

    private State state;

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
        try {
            feedAdapter.stop();
        } catch (Exception exception) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe("Unable to stop adapter " + feedAdapter + ", encountered exception " + exception);
            }
        } finally {
            state = State.FINISHED_INGESTION;
            executorService.shutdown();
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

    public IIntakeProgressTracker getTracker() {
        return tracker;
    }

    public synchronized State getState() {
        return state;
    }

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
