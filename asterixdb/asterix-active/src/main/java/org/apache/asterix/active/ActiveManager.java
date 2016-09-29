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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.apache.asterix.active.message.ActiveManagerMessage;
import org.apache.asterix.common.memory.ConcurrentFramePool;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.log4j.Logger;

public class ActiveManager {

    private static final Logger LOGGER = Logger.getLogger(ActiveManager.class.getName());
    private final Executor executor;
    private final Map<ActiveRuntimeId, IActiveRuntime> runtimes;
    private final ConcurrentFramePool activeFramePool;
    private final String nodeId;

    public ActiveManager(Executor executor, String nodeId, long activeMemoryBudget, int frameSize)
            throws HyracksDataException {
        this.executor = executor;
        this.nodeId = nodeId;
        this.activeFramePool = new ConcurrentFramePool(nodeId, activeMemoryBudget, frameSize);
        this.runtimes = new ConcurrentHashMap<>();
    }

    public ConcurrentFramePool getFramePool() {
        return activeFramePool;
    }

    public void registerRuntime(IActiveRuntime runtime) {
        ActiveRuntimeId id = runtime.getRuntimeId();
        if (!runtimes.containsKey(id)) {
            runtimes.put(id, runtime);
        }
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

    public void submit(ActiveManagerMessage message) {
        switch (message.getKind()) {
            case ActiveManagerMessage.STOP_ACTIVITY:
                stopRuntime(message);
                break;
            default:
                LOGGER.warn("Unknown message type received: " + message.getKind());
        }
    }

    private void stopRuntime(ActiveManagerMessage message) {
        ActiveRuntimeId runtimeId = (ActiveRuntimeId) message.getPayload();
        IActiveRuntime runtime = runtimes.get(runtimeId);
        if (runtime == null) {
            LOGGER.warn("Request to stop a runtime that is not registered " + runtimeId);
        } else {
            executor.execute(() -> {
                try {
                    runtime.stop();
                } catch (Exception e) {
                    // TODO(till) Figure out a better way to handle failure to stop a runtime
                    LOGGER.warn("Failed to stop runtime: " + runtimeId, e);
                }
            });
        }
    }
}
