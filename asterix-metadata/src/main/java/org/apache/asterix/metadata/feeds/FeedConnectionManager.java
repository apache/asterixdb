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

import org.apache.asterix.common.feeds.ActiveJobId;
import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedRuntime;
import org.apache.asterix.common.feeds.FeedRuntimeId;
import org.apache.asterix.common.feeds.FeedRuntimeManager;
import org.apache.asterix.common.feeds.api.IFeedConnectionManager;

/**
 * An implementation of the IFeedManager interface.
 * Provider necessary central repository for registering/retrieving
 * artifacts/services associated with a feed.
 */
public class FeedConnectionManager implements IFeedConnectionManager {

    private static final Logger LOGGER = Logger.getLogger(FeedConnectionManager.class.getName());

    private Map<FeedConnectionId, FeedRuntimeManager> feedRuntimeManagers = new HashMap<FeedConnectionId, FeedRuntimeManager>();
    private final String nodeId;

    public FeedConnectionManager(String nodeId) {
        this.nodeId = nodeId;
    }

    public FeedRuntimeManager getFeedRuntimeManager(ActiveJobId feedId) {
        return feedRuntimeManagers.get(feedId);
    }

    @Override
    public void deregisterFeed(ActiveJobId feedId) {
        try {
            FeedRuntimeManager mgr = feedRuntimeManagers.get(feedId);
            if (mgr != null) {
                mgr.close();
                feedRuntimeManagers.remove(feedId);
            }
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Exception in closing feed runtime" + e.getMessage());
            }
        }

    }

    @Override
    public synchronized void registerFeedRuntime(FeedConnectionId connectionId, FeedRuntime feedRuntime)
            throws Exception {
        FeedRuntimeManager runtimeMgr = feedRuntimeManagers.get(connectionId);
        if (runtimeMgr == null) {
            runtimeMgr = new FeedRuntimeManager(connectionId, this);
            feedRuntimeManagers.put(connectionId, runtimeMgr);
        }
        runtimeMgr.registerFeedRuntime(feedRuntime.getRuntimeId(), feedRuntime);
    }

    @Override
    public void deRegisterFeedRuntime(ActiveJobId connectionId, FeedRuntimeId feedRuntimeId) {
        FeedRuntimeManager runtimeMgr = feedRuntimeManagers.get(connectionId);
        if (runtimeMgr != null) {
            runtimeMgr.deregisterFeedRuntime(feedRuntimeId);
        }
    }

    @Override
    public FeedRuntime getFeedRuntime(ActiveJobId connectionId, FeedRuntimeId feedRuntimeId) {
        FeedRuntimeManager runtimeMgr = feedRuntimeManagers.get(connectionId);
        return runtimeMgr != null ? runtimeMgr.getFeedRuntime(feedRuntimeId) : null;
    }

    @Override
    public String toString() {
        return "FeedConnectionManager " + "[" + nodeId + "]";
    }

    @Override
    public List<FeedRuntimeId> getRegisteredRuntimes() {
        List<FeedRuntimeId> runtimes = new ArrayList<FeedRuntimeId>();
        for (Entry<FeedConnectionId, FeedRuntimeManager> entry : feedRuntimeManagers.entrySet()) {
            runtimes.addAll(entry.getValue().getFeedRuntimes());
        }
        return runtimes;
    }
}
