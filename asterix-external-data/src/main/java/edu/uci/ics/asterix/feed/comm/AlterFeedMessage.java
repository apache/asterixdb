/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.feed.comm;

import java.util.Map;

public class AlterFeedMessage extends FeedMessage {

    private final Map<String, String> alteredConfParams;

    public AlterFeedMessage(Map<String, String> alteredConfParams) {
        super(MessageType.ALTER);
        messageResponseMode = MessageResponseMode.SYNCHRONOUS;
        this.alteredConfParams = alteredConfParams;
    }

    public AlterFeedMessage(MessageResponseMode mode, Map<String, String> alteredConfParams) {
        super(MessageType.ALTER);
        messageResponseMode = mode;
        this.alteredConfParams = alteredConfParams;
    }

    @Override
    public MessageResponseMode getMessageResponseMode() {
        return messageResponseMode;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.ALTER;
    }

    public Map<String, String> getAlteredConfParams() {
        return alteredConfParams;
    }
}
