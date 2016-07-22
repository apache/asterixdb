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

import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ActiveManager {

    private final Map<ActiveRuntimeId, IActiveRuntime> runtimes;

    private final IActiveRuntimeRegistry activeRuntimeRegistry;

    private final ConcurrentFramePool activeFramePool;

    private final String nodeId;

    public ActiveManager(String nodeId, long activeMemoryBudget, int frameSize) throws HyracksDataException {
        this.nodeId = nodeId;
        this.activeRuntimeRegistry = new ActiveRuntimeRegistry(nodeId);
        this.activeFramePool = new ConcurrentFramePool(nodeId, activeMemoryBudget, frameSize);
        this.runtimes = new ConcurrentHashMap<>();
    }

    public IActiveRuntimeRegistry getActiveRuntimeRegistry() {
        return activeRuntimeRegistry;
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

    public IActiveRuntime getSubscribableRuntime(ActiveRuntimeId subscribableRuntimeId) {
        return runtimes.get(subscribableRuntimeId);
    }

    @Override
    public String toString() {
        return ActiveManager.class.getSimpleName() + "[" + nodeId + "]";
    }
}
