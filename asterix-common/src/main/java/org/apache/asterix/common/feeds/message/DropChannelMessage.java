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
package org.apache.asterix.common.feeds.message;

import org.apache.asterix.common.active.ActiveObjectId;
import org.apache.asterix.common.channels.ChannelRuntimeId;
import org.apache.asterix.common.feeds.FeedConstants;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A feed control message indicating the need to end the feed. This message is dispatched
 * to all locations that host an operator involved in the feed pipeline.
 */
public class DropChannelMessage extends ActiveMessage {

    private static final long serialVersionUID = 1L;

    private final ActiveObjectId channelId;

    private final ChannelRuntimeId channelRuntimeId;

    public DropChannelMessage(ActiveObjectId channelId, ChannelRuntimeId channelRuntimeId) {
        super(MessageType.DROP_CHANNEL);
        this.channelId = channelId;
        this.channelRuntimeId = channelRuntimeId;
    }

    @Override
    public String toString() {
        return MessageType.DROP_CHANNEL.name() + "  " + channelId;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(FeedConstants.MessageConstants.MESSAGE_TYPE, messageType.name());
        obj.put(FeedConstants.MessageConstants.DATAVERSE, channelId.getDataverse());
        obj.put(FeedConstants.MessageConstants.CHANNEL, channelId.getName());
        return obj;
    }

    public ActiveObjectId getChannelId() {
        return channelId;
    }

    public ChannelRuntimeId getChannelRuntimeId() {
        return channelRuntimeId;
    }

    /*   public static DropChannelMessage read(JSONObject obj) throws JSONException {
           ActiveId channelId = new ActiveId(obj.getString(FeedConstants.MessageConstants.DATAVERSE),
                   obj.getString(FeedConstants.MessageConstants.CHANNEL), ActiveObjectType.CHANNEL);
           return new DropChannelMessage(channelId, (ChannelRuntimeId) channelId);
       }*/

}
