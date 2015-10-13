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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.feeds.ActiveRuntime;
import org.apache.asterix.common.feeds.ActiveRuntimeManager;
import org.apache.asterix.common.feeds.api.ActiveRuntimeId;
import org.apache.asterix.common.feeds.api.IActiveConnectionManager;

/**
 * Provider necessary methods for retrieving
 * runtime managers for active jobs
 */
public class ActiveConnectionManager implements IActiveConnectionManager {

    private static final Logger LOGGER = Logger.getLogger(ActiveConnectionManager.class.getName());

    private Map<ActiveJobId, ActiveRuntimeManager> activeRuntimeManagers = new HashMap<ActiveJobId, ActiveRuntimeManager>();
    private final String nodeId;

    public ActiveConnectionManager(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public ActiveRuntimeManager getActiveRuntimeManager(ActiveJobId activeJobId) {
        return activeRuntimeManagers.get(activeJobId);
    }

    @Override
    public void deregister(ActiveJobId activeJobId) {
        try {
            ActiveRuntimeManager mgr = activeRuntimeManagers.get(activeJobId);
            if (mgr != null) {
                mgr.close();
                activeRuntimeManagers.remove(activeJobId);
            }
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Exception in closing runtime" + e.getMessage());
            }
        }

    }

    @Override
    public synchronized void registerActiveRuntime(ActiveJobId activeJobId, ActiveRuntime activeRuntime)
            throws Exception {
        ActiveRuntimeManager runtimeMgr = activeRuntimeManagers.get(activeJobId);
        if (runtimeMgr == null) {
            runtimeMgr = new ActiveRuntimeManager(activeJobId, this);
            activeRuntimeManagers.put(activeJobId, runtimeMgr);
        }
        runtimeMgr.registerRuntime(activeRuntime.getRuntimeId(), activeRuntime);
    }

    @Override
    public void deRegisterActiveRuntime(ActiveJobId activeJobId, ActiveRuntimeId activeRuntimeId) {
        ActiveRuntimeManager runtimeMgr = activeRuntimeManagers.get(activeJobId);
        if (runtimeMgr != null) {
            runtimeMgr.deregisterRuntime(activeRuntimeId);
        }
    }

    @Override
    public ActiveRuntime getActiveRuntime(ActiveJobId activeJobId, ActiveRuntimeId activeRuntimeId) {
        ActiveRuntimeManager runtimeMgr = activeRuntimeManagers.get(activeJobId);
        return runtimeMgr != null ? runtimeMgr.getActiveRuntime(activeRuntimeId) : null;
    }

    @Override
    public String toString() {
        return "ActiveConnectionManager " + "[" + nodeId + "]";
    }

    @Override
    public List<ActiveRuntimeId> getRegisteredRuntimes() {
        List<ActiveRuntimeId> runtimes = new ArrayList<ActiveRuntimeId>();
        for (Entry<ActiveJobId, ActiveRuntimeManager> entry : activeRuntimeManagers.entrySet()) {
            runtimes.addAll(entry.getValue().getRuntimes());
        }
        return runtimes;
    }
}
