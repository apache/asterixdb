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

import org.apache.asterix.common.active.ActiveObjectId;
import org.apache.asterix.common.active.ActiveObjectId.ActiveObjectType;
import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedConstants;
import org.apache.asterix.common.feeds.FeedConstants.MessageConstants;
import org.apache.asterix.common.feeds.api.ActiveRuntimeId;
import org.apache.asterix.common.feeds.api.IActiveRuntime.ActiveRuntimeType;
import org.apache.asterix.common.feeds.api.IFeedMetricCollector.ValueType;
import org.json.JSONException;
import org.json.JSONObject;

public class FeedReportMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final ActiveJobId activeJobId;
    private final ActiveRuntimeId runtimeId;
    private final ValueType valueType;
    private int value;

    public FeedReportMessage(ActiveJobId connectionId, ActiveRuntimeId runtimeId, ValueType valueType, int value) {
        super(MessageType.FEED_REPORT);
        this.activeJobId = connectionId;
        this.runtimeId = runtimeId;
        this.valueType = valueType;
        this.value = value;
    }

    public void reset(int value) {
        this.value = value;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(FeedConstants.MessageConstants.MESSAGE_TYPE, messageType.name());
        obj.put(FeedConstants.MessageConstants.DATAVERSE, activeJobId.getDataverse());
        obj.put(FeedConstants.MessageConstants.FEED, activeJobId.getName());
        if (activeJobId instanceof FeedConnectionId) {
            obj.put(FeedConstants.MessageConstants.DATASET, ((FeedConnectionId) activeJobId).getDatasetName());
        }
        obj.put(FeedConstants.MessageConstants.RUNTIME_TYPE, runtimeId.getRuntimeType());
        obj.put(FeedConstants.MessageConstants.PARTITION, runtimeId.getPartition());
        obj.put(FeedConstants.MessageConstants.VALUE_TYPE, valueType);
        obj.put(FeedConstants.MessageConstants.VALUE, value);
        return obj;
    }

    public static FeedReportMessage read(JSONObject obj) throws JSONException {
        ActiveObjectId feedId = new ActiveObjectId(obj.getString(FeedConstants.MessageConstants.DATAVERSE),
                obj.getString(FeedConstants.MessageConstants.FEED), ActiveObjectType.FEED);
        FeedConnectionId connectionId = new FeedConnectionId(feedId,
                obj.getString(FeedConstants.MessageConstants.DATASET));
        ActiveRuntimeId runtimeId = new ActiveRuntimeId(ActiveRuntimeType.valueOf(obj
                .getString(FeedConstants.MessageConstants.RUNTIME_TYPE)),
                obj.getInt(FeedConstants.MessageConstants.PARTITION), FeedConstants.MessageConstants.NOT_APPLICABLE);
        ValueType type = ValueType.valueOf(obj.getString(MessageConstants.VALUE_TYPE));
        int value = Integer.parseInt(obj.getString(MessageConstants.VALUE));
        return new FeedReportMessage(connectionId, runtimeId, type, value);
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public ActiveJobId getConnectionId() {
        return activeJobId;
    }

    public ActiveRuntimeId getRuntimeId() {
        return runtimeId;
    }

    public ValueType getValueType() {
        return valueType;
    }
}
