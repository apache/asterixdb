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

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.feeds.SubscribableFeedRuntimeId;
import org.apache.asterix.common.feeds.api.IFeedSubscriptionManager;
import org.apache.asterix.common.feeds.api.ISubscribableRuntime;

public class FeedSubscriptionManager implements IFeedSubscriptionManager {

    private static Logger LOGGER = Logger.getLogger(FeedSubscriptionManager.class.getName());

    private final String nodeId;

    private final Map<SubscribableFeedRuntimeId, ISubscribableRuntime> subscribableRuntimes;

    public FeedSubscriptionManager(String nodeId) {
        this.nodeId = nodeId;
        this.subscribableRuntimes = new HashMap<SubscribableFeedRuntimeId, ISubscribableRuntime>();
    }

    @Override
    public void registerFeedSubscribableRuntime(ISubscribableRuntime subscribableRuntime) {
        SubscribableFeedRuntimeId sid = (SubscribableFeedRuntimeId) subscribableRuntime.getRuntimeId();
        if (!subscribableRuntimes.containsKey(sid)) {
            subscribableRuntimes.put(sid, subscribableRuntime);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered feed subscribable runtime " + subscribableRuntime);
            }
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Feed ingestion runtime " + subscribableRuntime + " already registered.");
            }
        }
    }

    @Override
    public ISubscribableRuntime getSubscribableRuntime(SubscribableFeedRuntimeId subscribableFeedRuntimeId) {
        return subscribableRuntimes.get(subscribableFeedRuntimeId);
    }

    @Override
    public void deregisterFeedSubscribableRuntime(SubscribableFeedRuntimeId ingestionId) {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("De-registered feed subscribable runtime " + ingestionId);
        }
        subscribableRuntimes.remove(ingestionId);
    }

    @Override
    public String toString() {
        return "IngestionManager [" + nodeId + "]";
    }

}
