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

import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.api.IFeedManager;
import org.apache.asterix.common.feeds.api.IFeedMetadataManager;
import org.apache.asterix.hyracks.bootstrap.FeedBootstrap;
import org.apache.asterix.metadata.feeds.XAQLFeedMessage;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FeedMetadataManager implements IFeedMetadataManager {

    private static final Logger LOGGER = Logger.getLogger(FeedMetadataManager.class.getName());

    private final String nodeId;
    private ARecordType recordType;

    public FeedMetadataManager(String nodeId) throws AsterixException, HyracksDataException {
        this.nodeId = nodeId;
        String[] fieldNames = new String[] { "id", "dataverseName", "feedName", "targetDataset", "tuple", "message",
                "timestamp" };
        IAType[] fieldTypes = new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING };

        recordType = new ARecordType(FeedBootstrap.FAILED_TUPLE_DATASET_TYPE, fieldNames, fieldTypes, true);
    }

    @Override
    public void logTuple(FeedConnectionId connectionId, String tuple, String message, IFeedManager feedManager)
            throws AsterixException {
        try {
            AString id = new AString("1");
            AString dataverseValue = new AString(connectionId.getFeedId().getDataverse());
            AString feedValue = new AString(connectionId.getFeedId().getFeedName());
            AString targetDatasetValue = new AString(connectionId.getDatasetName());
            AString tupleValue = new AString(tuple);
            AString messageValue = new AString(message);
            AString dateTime = new AString(new Date().toString());

            IAObject[] fields = new IAObject[] { id, dataverseValue, feedValue, targetDatasetValue, tupleValue,
                    messageValue, dateTime };
            ARecord record = new ARecord(recordType, fields);
            StringBuilder builder = new StringBuilder();
            builder.append("use dataverse " + FeedBootstrap.FEEDS_METADATA_DV + ";" + "\n");
            builder.append("insert into dataset " + FeedBootstrap.FAILED_TUPLE_DATASET + " ");
            builder.append(" (" + recordToString(record) + ")");
            builder.append(";");

            XAQLFeedMessage xAqlMessage = new XAQLFeedMessage(connectionId, builder.toString());
            feedManager.getFeedMessageService().sendMessage(xAqlMessage);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(" Sent " + xAqlMessage.toJSON());
            }
        } catch (Exception pe) {
            throw new AsterixException(pe);
        }
    }

    @Override
    public String toString() {
        return "FeedMetadataManager [" + nodeId + "]";
    }

    private String recordToString(ARecord record) {
        String[] fieldNames = record.getType().getFieldNames();
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("\"" + fieldNames[i] + "\"");
            sb.append(": ");
            switch (record.getType().getFieldTypes()[i].getTypeTag()) {
                case STRING:
                    sb.append("\"" + ((AString) record.getValueByPos(i)).getStringValue() + "\"");
                    break;
            }
        }
        sb.append(" }");
        return sb.toString();
    }
}
