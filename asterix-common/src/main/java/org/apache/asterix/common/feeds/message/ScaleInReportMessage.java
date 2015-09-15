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
import org.apache.asterix.common.feeds.FeedId;
import org.apache.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;

/**
 * A feed control message indicating the need to scale in a stage of the feed ingestion pipeline.
 * Currently, scaling-in of the compute stage is supported.
 **/
public class ScaleInReportMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final FeedConnectionId connectionId;

    private final FeedRuntimeType runtimeType;

    private int currentCardinality;

    private int reducedCardinaliy;

    public ScaleInReportMessage(FeedConnectionId connectionId, FeedRuntimeType runtimeType, int currentCardinality,
            int reducedCardinaliy) {
        super(MessageType.SCALE_IN_REQUEST);
        this.connectionId = connectionId;
        this.runtimeType = runtimeType;
        this.currentCardinality = currentCardinality;
        this.reducedCardinaliy = reducedCardinaliy;
    }

    @Override
    public String toString() {
        return MessageType.SCALE_IN_REQUEST.name() + "  " + connectionId + " [" + runtimeType + "] "
                + " currentCardinality " + currentCardinality + " reducedCardinality " + reducedCardinaliy;
    }

    public FeedRuntimeType getRuntimeType() {
        return runtimeType;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(FeedConstants.MessageConstants.MESSAGE_TYPE, messageType.name());
        obj.put(FeedConstants.MessageConstants.DATAVERSE, connectionId.getFeedId().getDataverse());
        obj.put(FeedConstants.MessageConstants.FEED, connectionId.getFeedId().getFeedName());
        obj.put(FeedConstants.MessageConstants.DATASET, connectionId.getDatasetName());
        obj.put(FeedConstants.MessageConstants.RUNTIME_TYPE, runtimeType);
        obj.put(FeedConstants.MessageConstants.CURRENT_CARDINALITY, currentCardinality);
        obj.put(FeedConstants.MessageConstants.REDUCED_CARDINALITY, reducedCardinaliy);
        return obj;
    }

    public FeedConnectionId getConnectionId() {
        return connectionId;
    }

    public static ScaleInReportMessage read(JSONObject obj) throws JSONException {
        FeedId feedId = new FeedId(obj.getString(FeedConstants.MessageConstants.DATAVERSE),
                obj.getString(FeedConstants.MessageConstants.FEED));
        FeedConnectionId connectionId = new FeedConnectionId(feedId,
                obj.getString(FeedConstants.MessageConstants.DATASET));
        FeedRuntimeType runtimeType = FeedRuntimeType.valueOf(obj
                .getString(FeedConstants.MessageConstants.RUNTIME_TYPE));
        return new ScaleInReportMessage(connectionId, runtimeType,
                obj.getInt(FeedConstants.MessageConstants.CURRENT_CARDINALITY),
                obj.getInt(FeedConstants.MessageConstants.REDUCED_CARDINALITY));
    }

    public void reset(int currentCardinality, int reducedCardinaliy) {
        this.currentCardinality = currentCardinality;
        this.reducedCardinaliy = reducedCardinaliy;
    }

    public int getCurrentCardinality() {
        return currentCardinality;
    }

    public void setCurrentCardinality(int currentCardinality) {
        this.currentCardinality = currentCardinality;
    }

    public int getReducedCardinaliy() {
        return reducedCardinaliy;
    }

    public void setReducedCardinaliy(int reducedCardinaliy) {
        this.reducedCardinaliy = reducedCardinaliy;
    }

}
