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
import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedConstants;
import org.apache.asterix.common.feeds.message.ActiveMessage;

/**
 * A feed control message indicating the need to execute a give AQL.
 */
public class XAQLFeedMessage extends ActiveMessage {

    private static final long serialVersionUID = 1L;

    private final String aql;
    private final FeedConnectionId connectionId;

    public XAQLFeedMessage(FeedConnectionId connectionId, String aql) {
        super(MessageType.XAQL);
        this.connectionId = connectionId;
        this.aql = aql;
    }

    @Override
    public String toString() {
        return messageType.name() + " " + connectionId + " [" + aql + "] ";
    }

    public ActiveJobId getConnectionId() {
        return connectionId;
    }

    public String getAql() {
        return aql;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(FeedConstants.MessageConstants.MESSAGE_TYPE, messageType.name());
        obj.put(FeedConstants.MessageConstants.DATAVERSE, connectionId.getDataverse());
        obj.put(FeedConstants.MessageConstants.FEED, connectionId.getName());
        obj.put(FeedConstants.MessageConstants.DATASET, connectionId.getDatasetName());
        obj.put(FeedConstants.MessageConstants.AQL, aql);
        return obj;
    }

}
