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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of the {@code IActiveRuntimeRegistry} interface.
 * Provider necessary central repository for registering/retrieving
 * artifacts/services associated with an active entity.
 */
public class ActiveRuntimeRegistry implements IActiveRuntimeRegistry {

    private static final Logger LOGGER = Logger.getLogger(ActiveRuntimeRegistry.class.getName());

    private Map<ActiveRuntimeId, ActiveRuntimeManager> activeRuntimeManagers = new HashMap<>();
    private final String nodeId;

    public ActiveRuntimeRegistry(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void deregisterRuntime(ActiveRuntimeId runtimeId) {
        try {
            ActiveRuntimeManager mgr = activeRuntimeManagers.get(runtimeId);
            if (mgr != null) {
                mgr.deregisterRuntime(runtimeId);
                mgr.close();
                activeRuntimeManagers.remove(runtimeId);
            }
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.log(Level.WARNING, "Exception in closing feed runtime" + e.getMessage(), e);
            }
        }

    }

    @Override
    public synchronized void registerRuntime(ActiveRuntime runtime) {
        ActiveRuntimeManager runtimeMgr = activeRuntimeManagers.get(runtime.getRuntimeId());
        if (runtimeMgr == null) {
            runtimeMgr = new ActiveRuntimeManager();
            activeRuntimeManagers.put(runtime.getRuntimeId(), runtimeMgr);
        }
        runtimeMgr.registerRuntime(runtime.getRuntimeId(), runtime);
    }

    @Override
    public ActiveRuntime getRuntime(ActiveRuntimeId runtimeId) {
        ActiveRuntimeManager runtimeMgr = activeRuntimeManagers.get(runtimeId);
        return runtimeMgr != null ? runtimeMgr.getFeedRuntime(runtimeId) : null;
    }

    @Override
    public String toString() {
        return ActiveRuntimeRegistry.class.getSimpleName() + "[" + nodeId + "]";
    }
}
