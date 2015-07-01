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
package edu.uci.ics.asterix.common.feeds;

import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;

public class SubscribableFeedRuntimeId extends FeedRuntimeId {

    private final FeedId feedId;

    public SubscribableFeedRuntimeId(FeedId feedId, FeedRuntimeType runtimeType, int partition) {
        super(runtimeType, partition, FeedRuntimeId.DEFAULT_OPERAND_ID);
        this.feedId = feedId;
    }

    public FeedId getFeedId() {
        return feedId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SubscribableFeedRuntimeId)) {
            return false;
        }

        return (super.equals(o) && this.feedId.equals(((SubscribableFeedRuntimeId) o).getFeedId()));
    }

    @Override
    public int hashCode() {
        return super.hashCode() + feedId.hashCode();
    }
}
