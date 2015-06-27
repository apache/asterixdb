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
package edu.uci.ics.asterix.common.feeds.message;

import javax.xml.bind.DatatypeConverter;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedConstants;
import edu.uci.ics.asterix.common.feeds.FeedId;

public class FeedTupleCommitAckMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final FeedConnectionId connectionId;
    private int intakePartition;
    private int base;
    private byte[] commitAcks;

    public FeedTupleCommitAckMessage(FeedConnectionId connectionId, int intakePartition, int base, byte[] commitAcks) {
        super(MessageType.COMMIT_ACK);
        this.connectionId = connectionId;
        this.intakePartition = intakePartition;
        this.base = base;
        this.commitAcks = commitAcks;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(FeedConstants.MessageConstants.MESSAGE_TYPE, messageType.name());
        obj.put(FeedConstants.MessageConstants.DATAVERSE, connectionId.getFeedId().getDataverse());
        obj.put(FeedConstants.MessageConstants.FEED, connectionId.getFeedId().getFeedName());
        obj.put(FeedConstants.MessageConstants.DATASET, connectionId.getDatasetName());
        obj.put(FeedConstants.MessageConstants.INTAKE_PARTITION, intakePartition);
        obj.put(FeedConstants.MessageConstants.BASE, base);
        String commitAcksString = DatatypeConverter.printBase64Binary(commitAcks);
        obj.put(FeedConstants.MessageConstants.COMMIT_ACKS, commitAcksString);
        return obj;
    }

    public static FeedTupleCommitAckMessage read(JSONObject obj) throws JSONException {
        FeedId feedId = new FeedId(obj.getString(FeedConstants.MessageConstants.DATAVERSE),
                obj.getString(FeedConstants.MessageConstants.FEED));
        FeedConnectionId connectionId = new FeedConnectionId(feedId,
                obj.getString(FeedConstants.MessageConstants.DATASET));
        int intakePartition = obj.getInt(FeedConstants.MessageConstants.INTAKE_PARTITION);
        int base = obj.getInt(FeedConstants.MessageConstants.BASE);
        String commitAcksString = obj.getString(FeedConstants.MessageConstants.COMMIT_ACKS);
        byte[] commitAcks = DatatypeConverter.parseBase64Binary(commitAcksString);
        return new FeedTupleCommitAckMessage(connectionId, intakePartition, base, commitAcks);
    }

    public FeedConnectionId getConnectionId() {
        return connectionId;
    }

    public int getIntakePartition() {
        return intakePartition;
    }

    public byte[] getCommitAcks() {
        return commitAcks;
    }

    public void reset(int intakePartition, int base, byte[] commitAcks) {
        this.intakePartition = intakePartition;
        this.base = base;
        this.commitAcks = commitAcks;
    }

    public int getBase() {
        return base;
    }

    public void setBase(int base) {
        this.base = base;
    }

}
