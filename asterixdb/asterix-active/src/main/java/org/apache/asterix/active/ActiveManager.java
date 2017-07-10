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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.asterix.active.message.ActiveManagerMessage;
import org.apache.asterix.active.message.ActiveStatsResponse;
import org.apache.asterix.active.message.StatsRequestMessage;
import org.apache.asterix.common.api.ThreadExecutor;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.memory.ConcurrentFramePool;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.log4j.Logger;

public class ActiveManager {

    private static final Logger LOGGER = Logger.getLogger(ActiveManager.class.getName());
    private static final int SHUTDOWN_TIMEOUT_SECS = 60;

    private final ThreadExecutor executor;
    private final ConcurrentMap<ActiveRuntimeId, IActiveRuntime> runtimes;
    private final ConcurrentFramePool activeFramePool;
    private final String nodeId;
    private final INCServiceContext serviceCtx;
    private volatile boolean shutdown;

    public ActiveManager(ThreadExecutor executor, String nodeId, long activeMemoryBudget, int frameSize,
            INCServiceContext serviceCtx) throws HyracksDataException {
        this.executor = executor;
        this.nodeId = nodeId;
        this.activeFramePool = new ConcurrentFramePool(nodeId, activeMemoryBudget, frameSize);
        this.runtimes = new ConcurrentHashMap<>();
        this.serviceCtx = serviceCtx;
    }

    public ConcurrentFramePool getFramePool() {
        return activeFramePool;
    }

    public void registerRuntime(IActiveRuntime runtime) throws HyracksDataException {
        if (shutdown) {
            throw new RuntimeDataException(ErrorCode.ACTIVE_MANAGER_SHUTDOWN);
        }
        runtimes.putIfAbsent(runtime.getRuntimeId(), runtime);
    }

    public void deregisterRuntime(ActiveRuntimeId id) {
        runtimes.remove(id);
    }

    public IActiveRuntime getRuntime(ActiveRuntimeId runtimeId) {
        return runtimes.get(runtimeId);
    }

    @Override
    public String toString() {
        return ActiveManager.class.getSimpleName() + "[" + nodeId + "]";
    }

    public void submit(ActiveManagerMessage message) throws HyracksDataException {
        switch (message.getKind()) {
            case ActiveManagerMessage.STOP_ACTIVITY:
                stopRuntime(message);
                break;
            case ActiveManagerMessage.REQUEST_STATS:
                requestStats((StatsRequestMessage) message);
                break;
            default:
                LOGGER.warn("Unknown message type received: " + message.getKind());
        }
    }

    private void requestStats(StatsRequestMessage message) throws HyracksDataException {
        try {
            ActiveRuntimeId runtimeId = (ActiveRuntimeId) message.getPayload();
            IActiveRuntime runtime = runtimes.get(runtimeId);
            long reqId = message.getReqId();
            if (runtime == null) {
                LOGGER.warn("Request stats of a runtime that is not registered " + runtimeId);
                // Send a failure message
                ((NodeControllerService) serviceCtx.getControllerService())
                        .sendApplicationMessageToCC(
                                JavaSerializationUtils
                                        .serialize(new ActiveStatsResponse(reqId, null, new RuntimeDataException(
                                                ErrorCode.ACTIVE_MANAGER_INVALID_RUNTIME, runtimeId.toString()))),
                                null);
                return;
            }
            String stats = runtime.getStats();
            ActiveStatsResponse response = new ActiveStatsResponse(reqId, stats, null);
            ((NodeControllerService) serviceCtx.getControllerService())
                    .sendApplicationMessageToCC(JavaSerializationUtils.serialize(response), null);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    public void shutdown() {
        LOGGER.warn("Shutting down ActiveManager on node " + nodeId);
        Map<ActiveRuntimeId, Future<Void>> stopFutures = new HashMap<>();
        shutdown = true;
        runtimes.forEach((runtimeId, runtime) -> stopFutures.put(runtimeId, executor.submit(() -> {
            // we may already have been stopped- only stop once
            stopIfRunning(runtimeId, runtime);
            return null;
        })));
        stopFutures.entrySet().parallelStream().forEach(entry -> {
            try {
                entry.getValue().get(SHUTDOWN_TIMEOUT_SECS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted waiting to stop runtime: " + entry.getKey());
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                LOGGER.warn("Exception while stopping runtime: " + entry.getKey(), e);
            } catch (TimeoutException e) {
                LOGGER.warn("Timed out waiting to stop runtime: " + entry.getKey(), e);
            }
        });
        LOGGER.warn("Shutdown ActiveManager on node " + nodeId + " complete");
    }

    private void stopRuntime(ActiveManagerMessage message) {
        ActiveRuntimeId runtimeId = (ActiveRuntimeId) message.getPayload();
        IActiveRuntime runtime = runtimes.get(runtimeId);
        if (runtime == null) {
            LOGGER.warn("Request to stop a runtime that is not registered " + runtimeId);
        } else {
            executor.execute(() -> {
                try {
                    stopIfRunning(runtimeId, runtime);
                } catch (Exception e) {
                    // TODO(till) Figure out a better way to handle failure to stop a runtime
                    LOGGER.warn("Failed to stop runtime: " + runtimeId, e);
                }
            });
        }
    }

    private void stopIfRunning(ActiveRuntimeId runtimeId, IActiveRuntime runtime)
            throws HyracksDataException, InterruptedException {
        if (runtimes.remove(runtimeId) != null) {
            runtime.stop();
        } else {
            LOGGER.info("Not stopping already stopped runtime " + runtimeId);
        }
    }

}
