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
package org.apache.asterix.feeds;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.config.AsterixFeedProperties;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.FeedMemoryManager;
import org.apache.asterix.common.feeds.FeedMessageService;
import org.apache.asterix.common.feeds.FeedMetricCollector;
import org.apache.asterix.common.feeds.NodeLoadReportService;
import org.apache.asterix.common.feeds.api.IFeedConnectionManager;
import org.apache.asterix.common.feeds.api.IFeedManager;
import org.apache.asterix.common.feeds.api.IFeedMemoryManager;
import org.apache.asterix.common.feeds.api.IFeedMessageService;
import org.apache.asterix.common.feeds.api.IFeedMetadataManager;
import org.apache.asterix.common.feeds.api.IFeedMetricCollector;
import org.apache.asterix.common.feeds.api.IFeedSubscriptionManager;
import org.apache.asterix.metadata.feeds.FeedConnectionManager;
import org.apache.asterix.metadata.feeds.FeedSubscriptionManager;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * An implementation of the IFeedManager interface.
 * Provider necessary central repository for registering/retrieving
 * artifacts/services associated with a feed.
 */
public class FeedManager implements IFeedManager {

    private static final Logger LOGGER = Logger.getLogger(FeedManager.class.getName());

    private final IFeedSubscriptionManager feedSubscriptionManager;

    private final IFeedConnectionManager feedConnectionManager;

    private final IFeedMemoryManager feedMemoryManager;

    private final IFeedMetricCollector feedMetricCollector;

    private final IFeedMetadataManager feedMetadataManager;

    private final IFeedMessageService feedMessageService;

    private final NodeLoadReportService nodeLoadReportService;

    private final AsterixFeedProperties asterixFeedProperties;

    private final String nodeId;

    private final int frameSize;

    public FeedManager(String nodeId, AsterixFeedProperties feedProperties, int frameSize) throws AsterixException, HyracksDataException {
        this.nodeId = nodeId;
        this.feedSubscriptionManager = new FeedSubscriptionManager(nodeId);
        this.feedConnectionManager = new FeedConnectionManager(nodeId);
        this.feedMetadataManager = new FeedMetadataManager(nodeId);
        this.feedMemoryManager = new FeedMemoryManager(nodeId, feedProperties, frameSize);
        String ccClusterIp = AsterixClusterProperties.INSTANCE.getCluster() != null ? AsterixClusterProperties.INSTANCE
                .getCluster().getMasterNode().getClusterIp() : "localhost";
        this.feedMessageService = new FeedMessageService(feedProperties, nodeId, ccClusterIp);
        this.nodeLoadReportService = new NodeLoadReportService(nodeId, this);
        try {
            this.feedMessageService.start();
            this.nodeLoadReportService.start();
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to start feed services " + e.getMessage());
            }
            e.printStackTrace();
        }
        this.feedMetricCollector = new FeedMetricCollector(nodeId);
        this.frameSize = frameSize;
        this.asterixFeedProperties = feedProperties;
    }

    @Override
    public IFeedSubscriptionManager getFeedSubscriptionManager() {
        return feedSubscriptionManager;
    }

    @Override
    public IFeedConnectionManager getFeedConnectionManager() {
        return feedConnectionManager;
    }

    @Override
    public IFeedMemoryManager getFeedMemoryManager() {
        return feedMemoryManager;
    }

    @Override
    public IFeedMetricCollector getFeedMetricCollector() {
        return feedMetricCollector;
    }

    public int getFrameSize() {
        return frameSize;
    }

    @Override
    public IFeedMetadataManager getFeedMetadataManager() {
        return feedMetadataManager;
    }

    @Override
    public IFeedMessageService getFeedMessageService() {
        return feedMessageService;
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        return "FeedManager " + "[" + nodeId + "]";
    }

    @Override
    public AsterixFeedProperties getAsterixFeedProperties() {
        return asterixFeedProperties;
    }

}
