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
package org.apache.asterix.common.active;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.active.api.IActiveConnectionManager;

/**
 * Provider necessary methods for retrieving
 * runtimes for an active job
 */

public class ActiveRuntimeManager {

    private static Logger LOGGER = Logger.getLogger(ActiveRuntimeManager.class.getName());

    private final ActiveJobId activeJobId;
    private final IActiveConnectionManager connectionManager;
    private final Map<ActiveRuntimeId, ActiveRuntime> runtimes;

    private final ExecutorService executorService;

    public ActiveRuntimeManager(ActiveJobId activeJobId, IActiveConnectionManager feedConnectionManager) {
        this.activeJobId = activeJobId;
        this.runtimes = new ConcurrentHashMap<ActiveRuntimeId, ActiveRuntime>();
        this.executorService = Executors.newCachedThreadPool();
        this.connectionManager = feedConnectionManager;
    }

    public void close() throws IOException {
        if (executorService != null) {
            executorService.shutdownNow();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Shut down executor service for :" + activeJobId);
            }
        }
    }

    public ActiveRuntime getActiveRuntime(ActiveRuntimeId runtimeId) {
        return runtimes.get(runtimeId);
    }

    public void registerRuntime(ActiveRuntimeId runtimeId, ActiveRuntime runtime) {
        runtimes.put(runtimeId, runtime);
    }

    public synchronized void deregisterRuntime(ActiveRuntimeId runtimeId) {
        runtimes.remove(runtimeId);
        if (runtimes.isEmpty()) {
            connectionManager.deregister(activeJobId);
        }
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public Set<ActiveRuntimeId> getRuntimes() {
        return runtimes.keySet();
    }

}
