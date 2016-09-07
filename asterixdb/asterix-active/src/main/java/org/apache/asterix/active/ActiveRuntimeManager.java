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
package org.apache.asterix.active;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ActiveRuntimeManager {

    private static final Logger LOGGER = Logger.getLogger(ActiveRuntimeManager.class.getName());
    private final Map<ActiveRuntimeId, ActiveSourceOperatorNodePushable> activeRuntimes;

    private final ExecutorService executorService;

    public ActiveRuntimeManager() {
        this.activeRuntimes = new ConcurrentHashMap<>();
        this.executorService = Executors.newCachedThreadPool();
    }

    public void close() throws IOException {
        if (executorService != null) {
            executorService.shutdown();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Shut down executor service for :" + ActiveRuntimeManager.class.getSimpleName());
            }
            try {
                executorService.awaitTermination(10L, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.log(Level.SEVERE, ActiveRuntimeManager.class.getSimpleName()
                        + " was interrupted while waiting for runtime managers to shutdown", e);
            }
            if (!executorService.isTerminated()) {
                LOGGER.severe(ActiveRuntimeManager.class.getSimpleName()
                        + " failed to shutdown successfully. Will be forced to shutdown");
                executorService.shutdownNow();
            }
        }
    }

    public ActiveSourceOperatorNodePushable getRuntime(ActiveRuntimeId runtimeId) {
        return activeRuntimes.get(runtimeId);
    }

    public void registerRuntime(ActiveRuntimeId runtimeId, ActiveSourceOperatorNodePushable feedRuntime) {
        activeRuntimes.put(runtimeId, feedRuntime);
    }

    public synchronized void deregisterRuntime(ActiveRuntimeId runtimeId) {
        activeRuntimes.remove(runtimeId);
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public Set<ActiveRuntimeId> getFeedRuntimes() {
        return activeRuntimes.keySet();
    }

}
