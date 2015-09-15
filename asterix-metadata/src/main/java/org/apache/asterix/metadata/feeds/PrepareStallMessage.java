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

import org.json.JSONException;
import org.json.JSONObject;

import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedConstants;
import org.apache.asterix.common.feeds.message.FeedMessage;

/**
 * A feed control message indicating the need to end the feed. This message is dispatched
 * to all locations that host an operator involved in the feed pipeline.
 */
public class PrepareStallMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final FeedConnectionId connectionId;

    private final int computePartitionsRetainLimit;

    public PrepareStallMessage(FeedConnectionId connectionId, int computePartitionsRetainLimit) {
        super(MessageType.PREPARE_STALL);
        this.connectionId = connectionId;
        this.computePartitionsRetainLimit = computePartitionsRetainLimit;
    }

    @Override
    public String toString() {
        return MessageType.PREPARE_STALL.name() + "  " + connectionId;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(FeedConstants.MessageConstants.MESSAGE_TYPE, messageType.name());
        obj.put(FeedConstants.MessageConstants.DATAVERSE, connectionId.getFeedId().getDataverse());
        obj.put(FeedConstants.MessageConstants.FEED, connectionId.getFeedId().getFeedName());
        obj.put(FeedConstants.MessageConstants.DATASET, connectionId.getDatasetName());
        obj.put(FeedConstants.MessageConstants.COMPUTE_PARTITION_RETAIN_LIMIT, computePartitionsRetainLimit);
        return obj;
    }

    public FeedConnectionId getConnectionId() {
        return connectionId;
    }

    public int getComputePartitionsRetainLimit() {
        return computePartitionsRetainLimit;
    }

}
