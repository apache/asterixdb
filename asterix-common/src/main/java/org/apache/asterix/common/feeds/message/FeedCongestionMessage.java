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

import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedConstants;
import org.apache.asterix.common.feeds.FeedConstants.MessageConstants;
import org.apache.asterix.common.feeds.ActiveId;
import org.apache.asterix.common.feeds.FeedRuntimeId;
import org.apache.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import org.apache.asterix.common.feeds.api.IFeedRuntime.Mode;

public class FeedCongestionMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final FeedConnectionId connectionId;
    private final FeedRuntimeId runtimeId;
    private int inflowRate;
    private int outflowRate;
    private Mode mode;

    public FeedCongestionMessage(FeedConnectionId connectionId, FeedRuntimeId runtimeId, int inflowRate,
            int outflowRate, Mode mode) {
        super(MessageType.CONGESTION);
        this.connectionId = connectionId;
        this.runtimeId = runtimeId;
        this.inflowRate = inflowRate;
        this.outflowRate = outflowRate;
        this.mode = mode;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(FeedConstants.MessageConstants.MESSAGE_TYPE, messageType.name());
        obj.put(FeedConstants.MessageConstants.DATAVERSE, connectionId.getActiveId().getDataverse());
        obj.put(FeedConstants.MessageConstants.FEED, connectionId.getActiveId().getName());
        obj.put(FeedConstants.MessageConstants.DATASET, connectionId.getDatasetName());
        obj.put(FeedConstants.MessageConstants.RUNTIME_TYPE, runtimeId.getFeedRuntimeType());
        obj.put(FeedConstants.MessageConstants.OPERAND_ID, runtimeId.getOperandId());
        obj.put(FeedConstants.MessageConstants.PARTITION, runtimeId.getPartition());
        obj.put(FeedConstants.MessageConstants.INFLOW_RATE, inflowRate);
        obj.put(FeedConstants.MessageConstants.OUTFLOW_RATE, outflowRate);
        obj.put(FeedConstants.MessageConstants.MODE, mode);
        return obj;
    }

    public FeedRuntimeId getRuntimeId() {
        return runtimeId;
    }

    public int getInflowRate() {
        return inflowRate;
    }

    public int getOutflowRate() {
        return outflowRate;
    }

    public static FeedCongestionMessage read(JSONObject obj) throws JSONException {
        ActiveId feedId = new ActiveId(obj.getString(FeedConstants.MessageConstants.DATAVERSE),
                obj.getString(FeedConstants.MessageConstants.FEED));
        FeedConnectionId connectionId = new FeedConnectionId(feedId,
                obj.getString(FeedConstants.MessageConstants.DATASET));
        FeedRuntimeId runtimeId = new FeedRuntimeId(FeedRuntimeType.valueOf(obj
                .getString(FeedConstants.MessageConstants.RUNTIME_TYPE)),
                obj.getInt(FeedConstants.MessageConstants.PARTITION),
                obj.getString(FeedConstants.MessageConstants.OPERAND_ID));
        Mode mode = Mode.valueOf(obj.getString(MessageConstants.MODE));
        return new FeedCongestionMessage(connectionId, runtimeId,
                obj.getInt(FeedConstants.MessageConstants.INFLOW_RATE),
                obj.getInt(FeedConstants.MessageConstants.OUTFLOW_RATE), mode);
    }

    public FeedConnectionId getConnectionId() {
        return connectionId;
    }

    public Mode getMode() {
        return mode;
    }

}
