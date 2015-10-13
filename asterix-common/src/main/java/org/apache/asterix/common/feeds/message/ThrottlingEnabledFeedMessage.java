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

import org.apache.asterix.common.active.ActiveId;
import org.apache.asterix.common.active.ActiveId.ActiveObjectType;
import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.feeds.ActiveRuntimeId;
import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedConstants;
import org.apache.asterix.common.feeds.api.IActiveRuntime.ActiveRuntimeType;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This message is dispatched
 * to all locations that host an operator involved in the feed pipeline.
 */
public class ThrottlingEnabledFeedMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final FeedConnectionId connectionId;

    private final ActiveRuntimeId runtimeId;

    public ThrottlingEnabledFeedMessage(FeedConnectionId connectionId, ActiveRuntimeId runtimeId) {
        super(MessageType.THROTTLING_ENABLED);
        this.connectionId = connectionId;
        this.runtimeId = runtimeId;
    }

    @Override
    public String toString() {
        return MessageType.END.name() + "  " + connectionId + " [" + runtimeId + "] ";
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(FeedConstants.MessageConstants.MESSAGE_TYPE, messageType.name());
        obj.put(FeedConstants.MessageConstants.DATAVERSE, connectionId.getActiveId().getDataverse());
        obj.put(FeedConstants.MessageConstants.FEED, connectionId.getActiveId().getName());
        obj.put(FeedConstants.MessageConstants.DATASET, connectionId.getDatasetName());
        obj.put(FeedConstants.MessageConstants.RUNTIME_TYPE, runtimeId.getRuntimeType());
        obj.put(FeedConstants.MessageConstants.OPERAND_ID, runtimeId.getOperandId());
        obj.put(FeedConstants.MessageConstants.PARTITION, runtimeId.getPartition());
        return obj;
    }

    public ActiveJobId getConnectionId() {
        return connectionId;
    }

    public ActiveRuntimeId getFeedRuntimeId() {
        return runtimeId;
    }

    public static ThrottlingEnabledFeedMessage read(JSONObject obj) throws JSONException {
        ActiveId feedId = new ActiveId(obj.getString(FeedConstants.MessageConstants.DATAVERSE),
                obj.getString(FeedConstants.MessageConstants.FEED), ActiveObjectType.FEED);
        FeedConnectionId connectionId = new FeedConnectionId(feedId,
                obj.getString(FeedConstants.MessageConstants.DATASET));
        ActiveRuntimeId runtimeId = new ActiveRuntimeId(ActiveRuntimeType.valueOf(obj
                .getString(FeedConstants.MessageConstants.RUNTIME_TYPE)),
                obj.getInt(FeedConstants.MessageConstants.PARTITION),
                obj.getString(FeedConstants.MessageConstants.OPERAND_ID));
        return new ThrottlingEnabledFeedMessage(connectionId, runtimeId);
    }

}
