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
package org.apache.asterix.common.active.message;

import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.feeds.FeedConstants;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A feed control message indicating the need to execute a give AQL.
 */
public class RepetitiveChannelXAQLMessage extends ActiveMessage {

    private static final long serialVersionUID = 1L;

    private final String aql;
    private final ActiveJobId channelJobId;

    public RepetitiveChannelXAQLMessage(ActiveJobId channelJobId, String aql) {
        super(MessageType.XAQL);
        this.channelJobId = channelJobId;
        this.aql = aql;
    }

    @Override
    public String toString() {
        return messageType.name() + " " + channelJobId + " [" + aql + "] ";
    }

    public ActiveJobId getJobId() {
        return channelJobId;
    }

    public String getAql() {
        return aql;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(FeedConstants.MessageConstants.MESSAGE_TYPE, messageType.name());
        obj.put(FeedConstants.MessageConstants.DATAVERSE, channelJobId.getDataverse());
        obj.put(FeedConstants.MessageConstants.CHANNEL, channelJobId.getName());
        obj.put(FeedConstants.MessageConstants.AQL, aql);
        return obj;
    }

}