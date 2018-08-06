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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.asterix.active.message.ActiveManagerMessage;
import org.apache.asterix.active.message.ActiveStatsRequestMessage;
import org.apache.asterix.active.message.ActiveStatsResponse;
import org.apache.asterix.active.message.StopRuntimeParameters;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.memory.ConcurrentFramePool;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ActiveManager {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int SHUTDOWN_TIMEOUT_SECS = 60;

    private final ExecutorService executor;
    private final ConcurrentMap<ActiveRuntimeId, IActiveRuntime> runtimes;
    private final ConcurrentFramePool activeFramePool;
    private final String nodeId;
    private final INCServiceContext serviceCtx;
    private volatile boolean shutdown;

    public ActiveManager(ExecutorService executor, String nodeId, long activeMemoryBudget, int frameSize,
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
        if (runtimes.putIfAbsent(runtime.getRuntimeId(), runtime) != null) {
            throw new IllegalStateException("Active Runtime " + runtime.getRuntimeId() + " is already registered");
        }
    }

    public void deregisterRuntime(ActiveRuntimeId id) {
        runtimes.remove(id);
    }

    public Set<ActiveRuntimeId> getRuntimeIds() {
        return Collections.unmodifiableSet(runtimes.keySet());
    }

    public IActiveRuntime getRuntime(ActiveRuntimeId runtimeId) {
        return runtimes.get(runtimeId);
    }

    @Override
    public String toString() {
        return ActiveManager.class.getSimpleName() + "[" + nodeId + "]";
    }

    public void submit(ActiveManagerMessage message) throws HyracksDataException {
        LOGGER.log(Level.INFO, "Message of type " + message.getKind() + " received in " + nodeId);
        switch (message.getKind()) {
            case STOP_ACTIVITY:
                stopRuntime(message);
                break;
            case REQUEST_STATS:
                requestStats((ActiveStatsRequestMessage) message);
                break;
            default:
                LOGGER.warn("Unknown message type received: " + message.getKind());
        }
    }

    private void requestStats(ActiveStatsRequestMessage message) throws HyracksDataException {
        try {
            ActiveRuntimeId runtimeId = (ActiveRuntimeId) message.getPayload();
            IActiveRuntime runtime = runtimes.get(runtimeId);
            long reqId = message.getReqId();
            if (runtime == null) {
                LOGGER.warn("Request stats of a runtime that is not registered " + runtimeId);
                // Send a failure message
                ((NodeControllerService) serviceCtx.getControllerService()).sendApplicationMessageToCC(
                        message.getCcId(),
                        JavaSerializationUtils.serialize(new ActiveStatsResponse(reqId, null, new RuntimeDataException(
                                ErrorCode.ACTIVE_MANAGER_INVALID_RUNTIME, runtimeId.toString()))),
                        null);
                return;
            }
            String stats = runtime.getStats();
            ActiveStatsResponse response = new ActiveStatsResponse(reqId, stats, null);
            ((NodeControllerService) serviceCtx.getControllerService()).sendApplicationMessageToCC(message.getCcId(),
                    JavaSerializationUtils.serialize(response), null);
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
            stopIfRunning(runtime, SHUTDOWN_TIMEOUT_SECS, TimeUnit.SECONDS);
            return null;
        })));
        stopFutures.entrySet().parallelStream().forEach(entry -> {
            try {
                entry.getValue().get(SHUTDOWN_TIMEOUT_SECS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted waiting to stop runtime: " + entry.getKey());
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                LOGGER.log(Level.WARN, "Exception while stopping runtime: " + entry.getKey(), e);
            } catch (TimeoutException e) {
                LOGGER.log(Level.WARN, "Timed out waiting to stop runtime: " + entry.getKey(), e);
            }
        });
        LOGGER.warn("Shutdown ActiveManager on node " + nodeId + " complete");
    }

    @SuppressWarnings("squid:S1181") // Catch Error
    private void stopRuntime(ActiveManagerMessage message) {
        StopRuntimeParameters content = (StopRuntimeParameters) message.getPayload();
        ActiveRuntimeId runtimeId = content.getRuntimeId();
        IActiveRuntime runtime = runtimes.get(runtimeId);
        if (runtime == null) {
            LOGGER.warn("Request to stop runtime: " + runtimeId
                    + " that is not registered. Could be that the runtime completed execution on"
                    + " this node before the cluster controller sent the stop request");
        } else {
            executor.execute(() -> {
                try {
                    stopIfRunning(runtime, content.getTimeout(), content.getUnit());
                } catch (Throwable th) {
                    LOGGER.warn("Failed to stop runtime: {}", runtimeId, th);
                    ExitUtil.halt(ExitUtil.EC_UNCAUGHT_THROWABLE);
                }
            });
        }
    }

    private void stopIfRunning(IActiveRuntime runtime, long timeout, TimeUnit unit)
            throws HyracksDataException, InterruptedException {
        if (runtimes.containsKey(runtime.getRuntimeId())) {
            runtime.stop(timeout, unit);
        } else {
            LOGGER.info("Not stopping already stopped runtime {}", runtime.getRuntimeId());
        }
    }

}
