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

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.rmi.RemoteException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.active.api.IActiveConnectionManager;
import org.apache.asterix.common.active.api.IActiveManager;
import org.apache.asterix.common.config.AsterixFeedProperties;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.FeedMemoryManager;
import org.apache.asterix.common.feeds.FeedMessageService;
import org.apache.asterix.common.feeds.FeedMetricCollector;
import org.apache.asterix.common.feeds.NodeLoadReportService;
import org.apache.asterix.common.feeds.api.IFeedMemoryManager;
import org.apache.asterix.common.feeds.api.IFeedMessageService;
import org.apache.asterix.common.feeds.api.IFeedMetadataManager;
import org.apache.asterix.common.feeds.api.IFeedMetricCollector;
import org.apache.asterix.common.feeds.api.IFeedSubscriptionManager;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.active.ActiveConnectionManager;
import org.apache.asterix.metadata.entities.Broker;
import org.apache.asterix.metadata.feeds.FeedSubscriptionManager;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * An implementation of the IFeedManager interface.
 * Provider necessary central repository for registering/retrieving
 * artifacts/services associated with a feed.
 */
public class ActiveManager implements IActiveManager {

    private static final Logger LOGGER = Logger.getLogger(ActiveManager.class.getName());

    private final IFeedSubscriptionManager feedSubscriptionManager;

    private final IActiveConnectionManager connectionManager;

    private final IFeedMemoryManager feedMemoryManager;

    private final IFeedMetricCollector feedMetricCollector;

    private final IFeedMetadataManager feedMetadataManager;

    private final IFeedMessageService feedMessageService;

    private final NodeLoadReportService nodeLoadReportService;

    private final AsterixFeedProperties asterixFeedProperties;

    private final String nodeId;

    private final int frameSize;

    public ActiveManager(String nodeId, AsterixFeedProperties feedProperties, int frameSize)
            throws AsterixException, HyracksDataException {
        this.nodeId = nodeId;
        this.feedSubscriptionManager = new FeedSubscriptionManager(nodeId);
        this.connectionManager = new ActiveConnectionManager(nodeId);
        this.feedMetadataManager = new FeedMetadataManager(nodeId);
        this.feedMemoryManager = new FeedMemoryManager(nodeId, feedProperties, frameSize);
        String ccClusterIp = AsterixClusterProperties.INSTANCE.getCluster() != null
                ? AsterixClusterProperties.INSTANCE.getCluster().getMasterNode().getClusterIp() : "localhost";
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
    public IActiveConnectionManager getConnectionManager() {
        return connectionManager;
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

    //TODO: Move these things to a new class for sending messages to Brokers
    @Override
    public void sendHttpForChannel() throws RemoteException, ACIDException, AsterixException {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();

        List<Broker> brokers;
        try {
            brokers = MetadataManager.INSTANCE.getBrokers(mdTxnCtx);
        } catch (MetadataException e) {
            throw new AsterixException("unable to read brokers from metadata", e);
        }
        for (Broker b : brokers) {
            String endPoint = b.getEndPointName();
            sendMessage(endPoint, "newResults");
        }
    }

    public static void sendMessage(String targetURL, String urlParameters) {
        HttpURLConnection connection = null;
        try {
            //Create connection
            URL url = new URL(targetURL);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            connection.setRequestProperty("Content-Length", Integer.toString(urlParameters.getBytes().length));
            connection.setRequestProperty("Content-Language", "en-US");

            connection.setUseCaches(false);
            connection.setDoOutput(true);

            //Send message
            DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
            wr.writeBytes(urlParameters);
            wr.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

}
