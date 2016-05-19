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
package org.apache.asterix.external.feed.management;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.asterix.common.config.AsterixFeedProperties;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.feed.api.IFeedConnectionManager;
import org.apache.asterix.external.feed.api.ISubscribableRuntime;
import org.apache.asterix.external.feed.runtime.FeedRuntimeId;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * An implementation of the IFeedManager interface.
 * Provider necessary central repository for registering/retrieving
 * artifacts/services associated with a feed.
 */
public class FeedManager {

    private final Map<FeedRuntimeId, ISubscribableRuntime> subscribableRuntimes;

    private final IFeedConnectionManager feedConnectionManager;

    private final ConcurrentFramePool feedMemoryManager;

    private final AsterixFeedProperties asterixFeedProperties;

    private final String nodeId;

    private final int frameSize;

    public FeedManager(String nodeId, AsterixFeedProperties feedProperties, int frameSize)
            throws AsterixException, HyracksDataException {
        this.nodeId = nodeId;
        this.feedConnectionManager = new FeedConnectionManager(nodeId);
        this.feedMemoryManager =
                new ConcurrentFramePool(nodeId, feedProperties.getMemoryComponentGlobalBudget(), frameSize);
        this.frameSize = frameSize;
        this.asterixFeedProperties = feedProperties;
        this.subscribableRuntimes = new ConcurrentHashMap<FeedRuntimeId, ISubscribableRuntime>();
    }

    public IFeedConnectionManager getFeedConnectionManager() {
        return feedConnectionManager;
    }

    public ConcurrentFramePool getFeedMemoryManager() {
        return feedMemoryManager;
    }

    public int getFrameSize() {
        return frameSize;
    }

    public void registerFeedSubscribableRuntime(ISubscribableRuntime subscribableRuntime) {
        FeedRuntimeId sid = subscribableRuntime.getRuntimeId();
        if (!subscribableRuntimes.containsKey(sid)) {
            subscribableRuntimes.put(sid, subscribableRuntime);
        }
    }

    public void deregisterFeedSubscribableRuntime(FeedRuntimeId subscribableRuntimeId) {
        subscribableRuntimes.remove(subscribableRuntimeId);
    }

    public ISubscribableRuntime getSubscribableRuntime(FeedRuntimeId subscribableRuntimeId) {
        return subscribableRuntimes.get(subscribableRuntimeId);
    }

    @Override
    public String toString() {
        return "FeedManager " + "[" + nodeId + "]";
    }

    public AsterixFeedProperties getAsterixFeedProperties() {
        return asterixFeedProperties;
    }

}
