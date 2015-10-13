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
package org.apache.asterix.metadata.channels;

import org.json.JSONException;
import org.json.JSONObject;

import org.apache.asterix.common.channels.ChannelId;
import org.apache.asterix.common.feeds.FeedConstants;
import org.apache.asterix.common.feeds.message.FeedMessage;

/**
 * A feed control message indicating the need to execute a give AQL.
 */
public class RepetitiveChannelXAQLMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final String aql;
    private final ChannelId channelId;

    public RepetitiveChannelXAQLMessage(ChannelId channelId, String aql) {
        super(MessageType.XAQL);
        this.channelId = channelId;
        this.aql = aql;
    }

    @Override
    public String toString() {
        return messageType.name() + " " + channelId + " [" + aql + "] ";
    }

    public ChannelId getConnectionId() {
        return channelId;
    }

    public String getAql() {
        return aql;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(FeedConstants.MessageConstants.MESSAGE_TYPE, messageType.name());
        obj.put(FeedConstants.MessageConstants.DATAVERSE, channelId.getDataverse());
        obj.put(FeedConstants.MessageConstants.FEED, channelId.getChannelName());
        obj.put(FeedConstants.MessageConstants.AQL, aql);
        return obj;
    }

}