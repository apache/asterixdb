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
import org.apache.asterix.common.feeds.FeedConstants.MessageConstants;
import org.apache.asterix.common.feeds.ActiveId;

/**
 * A feed control message sent from a storage runtime of a feed pipeline to report the intake timestamp corresponding
 * to the last persisted tuple.
 */
public class StorageReportFeedMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final FeedConnectionId connectionId;
    private final int partition;
    private long lastPersistedTupleIntakeTimestamp;
    private boolean persistenceDelayWithinLimit;
    private long averageDelay;
    private int intakePartition;

    public StorageReportFeedMessage(FeedConnectionId connectionId, int partition,
            long lastPersistedTupleIntakeTimestamp, boolean persistenceDelayWithinLimit, long averageDelay,
            int intakePartition) {
        super(MessageType.STORAGE_REPORT);
        this.connectionId = connectionId;
        this.partition = partition;
        this.lastPersistedTupleIntakeTimestamp = lastPersistedTupleIntakeTimestamp;
        this.persistenceDelayWithinLimit = persistenceDelayWithinLimit;
        this.averageDelay = averageDelay;
        this.intakePartition = intakePartition;
    }

    @Override
    public String toString() {
        return messageType.name() + " " + connectionId + " [" + lastPersistedTupleIntakeTimestamp + "] ";
    }

    public ActiveJobId getConnectionId() {
        return connectionId;
    }

    public long getLastPersistedTupleIntakeTimestamp() {
        return lastPersistedTupleIntakeTimestamp;
    }

    public int getPartition() {
        return partition;
    }

    public boolean isPersistenceDelayWithinLimit() {
        return persistenceDelayWithinLimit;
    }

    public void setPersistenceDelayWithinLimit(boolean persistenceDelayWithinLimit) {
        this.persistenceDelayWithinLimit = persistenceDelayWithinLimit;
    }

    public long getAverageDelay() {
        return averageDelay;
    }

    public void setAverageDelay(long averageDelay) {
        this.averageDelay = averageDelay;
    }

    public int getIntakePartition() {
        return intakePartition;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(FeedConstants.MessageConstants.MESSAGE_TYPE, messageType.name());
        obj.put(FeedConstants.MessageConstants.DATAVERSE, connectionId.getActiveId().getDataverse());
        obj.put(FeedConstants.MessageConstants.FEED, connectionId.getActiveId().getName());
        obj.put(FeedConstants.MessageConstants.DATASET, connectionId.getDatasetName());
        obj.put(FeedConstants.MessageConstants.LAST_PERSISTED_TUPLE_INTAKE_TIMESTAMP, lastPersistedTupleIntakeTimestamp);
        obj.put(MessageConstants.PERSISTENCE_DELAY_WITHIN_LIMIT, persistenceDelayWithinLimit);
        obj.put(MessageConstants.AVERAGE_PERSISTENCE_DELAY, averageDelay);
        obj.put(FeedConstants.MessageConstants.PARTITION, partition);
        obj.put(FeedConstants.MessageConstants.INTAKE_PARTITION, intakePartition);

        return obj;
    }

    public static StorageReportFeedMessage read(JSONObject obj) throws JSONException {
        ActiveId feedId = new ActiveId(obj.getString(FeedConstants.MessageConstants.DATAVERSE),
                obj.getString(FeedConstants.MessageConstants.FEED));
        FeedConnectionId connectionId = new FeedConnectionId(feedId,
                obj.getString(FeedConstants.MessageConstants.DATASET));
        int partition = obj.getInt(FeedConstants.MessageConstants.PARTITION);
        long timestamp = obj.getLong(FeedConstants.MessageConstants.LAST_PERSISTED_TUPLE_INTAKE_TIMESTAMP);
        boolean persistenceDelayWithinLimit = obj.getBoolean(MessageConstants.PERSISTENCE_DELAY_WITHIN_LIMIT);
        long averageDelay = obj.getLong(MessageConstants.AVERAGE_PERSISTENCE_DELAY);
        int intakePartition = obj.getInt(MessageConstants.INTAKE_PARTITION);
        return new StorageReportFeedMessage(connectionId, partition, timestamp, persistenceDelayWithinLimit,
                averageDelay, intakePartition);
    }

    public void reset(long lastPersistedTupleIntakeTimestamp, boolean delayWithinLimit, long averageDelay) {
        this.lastPersistedTupleIntakeTimestamp = lastPersistedTupleIntakeTimestamp;
        this.persistenceDelayWithinLimit = delayWithinLimit;
        this.averageDelay = averageDelay;
    }

}
