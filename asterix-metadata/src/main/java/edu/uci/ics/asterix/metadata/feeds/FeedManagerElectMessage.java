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
package edu.uci.ics.asterix.metadata.feeds;

/**
 * A feed control message containing the altered values for
 * adapter configuration parameters. This message is dispatched
 * to all runtime instances of the feed's adapter.
 */
public class FeedManagerElectMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final SuperFeedManager superFeedMaanger;

    public FeedManagerElectMessage(SuperFeedManager superFeedManager) {
        super(MessageType.SUPER_FEED_MANAGER_ELECT, superFeedManager.getFeedConnectionId());
        this.superFeedMaanger = superFeedManager;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.SUPER_FEED_MANAGER_ELECT;
    }

    public SuperFeedManager getSuperFeedMaanger() {
        return superFeedMaanger;
    }

    @Override
    public String toString() {
        return superFeedMaanger.toString();
    }

}
