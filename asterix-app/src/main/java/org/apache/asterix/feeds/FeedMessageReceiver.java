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

import org.json.JSONObject;

import org.apache.asterix.common.feeds.FeedConstants;
import org.apache.asterix.common.feeds.FeedTupleCommitAckMessage;
import org.apache.asterix.common.feeds.MessageReceiver;
import org.apache.asterix.common.feeds.NodeLoadReport;
import org.apache.asterix.common.feeds.api.IFeedLoadManager;
import org.apache.asterix.common.feeds.api.IFeedMessage.MessageType;
import org.apache.asterix.common.feeds.api.IFeedTrackingManager;
import org.apache.asterix.common.feeds.message.FeedCongestionMessage;
import org.apache.asterix.common.feeds.message.FeedReportMessage;
import org.apache.asterix.common.feeds.message.ScaleInReportMessage;
import org.apache.asterix.common.feeds.message.StorageReportFeedMessage;
import org.apache.asterix.common.feeds.message.ThrottlingEnabledFeedMessage;
import org.apache.asterix.feeds.CentralFeedManager.AQLExecutor;
import org.apache.asterix.hyracks.bootstrap.FeedBootstrap;

public class FeedMessageReceiver extends MessageReceiver<String> {

    private static boolean initialized;

    private final IFeedLoadManager feedLoadManager;
    private final IFeedTrackingManager feedTrackingManager;

    public FeedMessageReceiver(CentralFeedManager centralFeedManager) {
        this.feedLoadManager = centralFeedManager.getFeedLoadManager();
        this.feedTrackingManager = centralFeedManager.getFeedTrackingManager();
    }

    @Override
    public void processMessage(String message) throws Exception {
        JSONObject obj = new JSONObject(message);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Received message " + obj);
        }
        MessageType messageType = MessageType.valueOf(obj.getString(FeedConstants.MessageConstants.MESSAGE_TYPE));
        switch (messageType) {
            case XAQL:
                if (!initialized) {
                    FeedBootstrap.setUpInitialArtifacts();
                    initialized = true;
                }
                AQLExecutor.executeAQL(obj.getString(FeedConstants.MessageConstants.AQL));
                break;
            case CONGESTION:
                feedLoadManager.reportCongestion(FeedCongestionMessage.read(obj));
                break;
            case FEED_REPORT:
                feedLoadManager.submitFeedRuntimeReport(FeedReportMessage.read(obj));
                break;
            case NODE_REPORT:
                feedLoadManager.submitNodeLoadReport(NodeLoadReport.read(obj));
                break;
            case SCALE_IN_REQUEST:
                feedLoadManager.submitScaleInPossibleReport(ScaleInReportMessage.read(obj));
                break;
            case STORAGE_REPORT:
                FeedLifecycleListener.INSTANCE.updateTrackingInformation(StorageReportFeedMessage.read(obj));
                break;
            case COMMIT_ACK:
                feedTrackingManager.submitAckReport(FeedTupleCommitAckMessage.read(obj));
                break;
            case THROTTLING_ENABLED:
                feedLoadManager.reportThrottlingEnabled(ThrottlingEnabledFeedMessage.read(obj));
            default:
                break;
        }

    }
}
