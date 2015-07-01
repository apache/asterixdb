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

import edu.uci.ics.asterix.common.feeds.FeedConnectionId;

/**
 * A feed control message containing the altered values for
 * adapter configuration parameters. This message is dispatched
 * to all runtime instances of the feed's adapter.
 */
public class FeedManagerElectMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final String host;
    private final String nodeId;
    private final int port;

    public FeedManagerElectMessage(String host, String nodeId, int port, FeedConnectionId feedId) {
        super(MessageType.SUPER_FEED_MANAGER_ELECT, feedId);
        this.host = host;
        this.port = port;
        this.nodeId = nodeId;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.SUPER_FEED_MANAGER_ELECT;
    }

    @Override
    public String toString() {
        return MessageType.SUPER_FEED_MANAGER_ELECT.name() + " " + host + "_" + nodeId + "[" + port + "]";
    }

    public String getHost() {
        return host;
    }

    public String getNodeId() {
        return nodeId;
    }

    public int getPort() {
        return port;
    }

}
