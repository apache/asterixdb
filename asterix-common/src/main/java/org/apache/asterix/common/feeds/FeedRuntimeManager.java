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
package org.apache.asterix.common.feeds;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.feeds.api.IFeedConnectionManager;

public class FeedRuntimeManager {

    private static Logger LOGGER = Logger.getLogger(FeedRuntimeManager.class.getName());

    private final FeedConnectionId connectionId;
    private final IFeedConnectionManager connectionManager;
    private final Map<FeedRuntimeId, FeedRuntime> feedRuntimes;

    private final ExecutorService executorService;

    public FeedRuntimeManager(FeedConnectionId connectionId, IFeedConnectionManager feedConnectionManager) {
        this.connectionId = connectionId;
        this.feedRuntimes = new ConcurrentHashMap<FeedRuntimeId, FeedRuntime>();
        this.executorService = Executors.newCachedThreadPool();
        this.connectionManager = feedConnectionManager;
    }

    public void close() throws IOException {
        if (executorService != null) {
            executorService.shutdownNow();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Shut down executor service for :" + connectionId);
            }
        }
    }

    public FeedRuntime getFeedRuntime(FeedRuntimeId runtimeId) {
        return feedRuntimes.get(runtimeId);
    }

    public void registerFeedRuntime(FeedRuntimeId runtimeId, FeedRuntime feedRuntime) {
        feedRuntimes.put(runtimeId, feedRuntime);
    }

    public synchronized void deregisterFeedRuntime(FeedRuntimeId runtimeId) {
        feedRuntimes.remove(runtimeId);
        if (feedRuntimes.isEmpty()) {
            connectionManager.deregisterFeed(connectionId);
        }
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public Set<FeedRuntimeId> getFeedRuntimes() {
        return feedRuntimes.keySet();
    }

}
