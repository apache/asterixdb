/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.common.feeds;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.feeds.message.FeedMessage;

public class FeedTupleCommitResponseMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final FeedConnectionId connectionId;
    private final int intakePartition;
    private final int maxWindowAcked;

    public FeedTupleCommitResponseMessage(FeedConnectionId connectionId, int intakePartition, int maxWindowAcked) {
        super(MessageType.COMMIT_ACK_RESPONSE);
        this.connectionId = connectionId;
        this.intakePartition = intakePartition;
        this.maxWindowAcked = maxWindowAcked;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(FeedConstants.MessageConstants.MESSAGE_TYPE, messageType.name());
        obj.put(FeedConstants.MessageConstants.DATAVERSE, connectionId.getFeedId().getDataverse());
        obj.put(FeedConstants.MessageConstants.FEED, connectionId.getFeedId().getFeedName());
        obj.put(FeedConstants.MessageConstants.DATASET, connectionId.getDatasetName());
        obj.put(FeedConstants.MessageConstants.INTAKE_PARTITION, intakePartition);
        obj.put(FeedConstants.MessageConstants.MAX_WINDOW_ACKED, maxWindowAcked);
        return obj;
    }

    @Override
    public String toString() {
        return connectionId + "[" + intakePartition + "]" + "(" + maxWindowAcked + ")";
    }

    public static FeedTupleCommitResponseMessage read(JSONObject obj) throws JSONException {
        FeedId feedId = new FeedId(obj.getString(FeedConstants.MessageConstants.DATAVERSE),
                obj.getString(FeedConstants.MessageConstants.FEED));
        FeedConnectionId connectionId = new FeedConnectionId(feedId,
                obj.getString(FeedConstants.MessageConstants.DATASET));
        int intakePartition = obj.getInt(FeedConstants.MessageConstants.INTAKE_PARTITION);
        int maxWindowAcked = obj.getInt(FeedConstants.MessageConstants.MAX_WINDOW_ACKED);
        return new FeedTupleCommitResponseMessage(connectionId, intakePartition, maxWindowAcked);
    }

    public FeedConnectionId getConnectionId() {
        return connectionId;
    }

    public int getMaxWindowAcked() {
        return maxWindowAcked;
    }

    public int getIntakePartition() {
        return intakePartition;
    }

}
