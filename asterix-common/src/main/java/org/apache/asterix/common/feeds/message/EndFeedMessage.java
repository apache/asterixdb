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
package org.apache.asterix.common.feeds.message;

import org.json.JSONException;
import org.json.JSONObject;
import org.apache.asterix.common.feeds.ActiveJobId;
import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedConstants;
import org.apache.asterix.common.feeds.ActiveId;
import org.apache.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;

/**
 * A feed control message indicating the need to end the feed. This message is dispatched
 * to all locations that host an operator involved in the feed pipeline.
 */
public class EndFeedMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final ActiveId sourceFeedId;

    private final FeedConnectionId connectionId;

    private final FeedRuntimeType sourceRuntimeType;

    private final boolean completeDisconnection;

    private final EndMessageType endMessageType;

    public enum EndMessageType {
        DISCONNECT_FEED,
        DISCONTINUE_SOURCE
    }

    public EndFeedMessage(FeedConnectionId connectionId, FeedRuntimeType sourceRuntimeType, ActiveId sourceFeedId,
            boolean completeDisconnection, EndMessageType endMessageType) {
        super(MessageType.END);
        this.connectionId = connectionId;
        this.sourceRuntimeType = sourceRuntimeType;
        this.sourceFeedId = sourceFeedId;
        this.completeDisconnection = completeDisconnection;
        this.endMessageType = endMessageType;
    }

    @Override
    public String toString() {
        return MessageType.END.name() + "  " + connectionId + " [" + sourceRuntimeType + "] ";
    }

    public FeedRuntimeType getSourceRuntimeType() {
        return sourceRuntimeType;
    }

    public ActiveId getSourceFeedId() {
        return sourceFeedId;
    }

    public boolean isCompleteDisconnection() {
        return completeDisconnection;
    }

    public EndMessageType getEndMessageType() {
        return endMessageType;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(FeedConstants.MessageConstants.MESSAGE_TYPE, messageType.name());
        obj.put(FeedConstants.MessageConstants.DATAVERSE, connectionId.getActiveId().getDataverse());
        obj.put(FeedConstants.MessageConstants.FEED, connectionId.getActiveId().getName());
        obj.put(FeedConstants.MessageConstants.DATASET, connectionId.getDatasetName());
        return obj;
    }

    public ActiveJobId getFeedConnectionId() {
        return connectionId;
    }

}
