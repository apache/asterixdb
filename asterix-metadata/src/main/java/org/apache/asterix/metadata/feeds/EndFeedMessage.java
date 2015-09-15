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

import org.apache.asterix.common.feeds.FeedConnectionId;

/**
 * A feed control message indicating the need to end the feed. This message is dispatched
 * to all locations that host an operator invovled in the feed pipeline.
 */
public class EndFeedMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final FeedConnectionId feedId;

    public EndFeedMessage(FeedConnectionId feedId) {
        super(MessageType.END, feedId);
        this.feedId = feedId;
    }

    @Override
    public String toString() {
        return MessageType.END.name() + feedId;
    }
}
