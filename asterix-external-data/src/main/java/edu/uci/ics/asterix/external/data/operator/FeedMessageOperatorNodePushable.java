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
package edu.uci.ics.asterix.external.data.operator;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.external.feed.lifecycle.FeedId;
import edu.uci.ics.asterix.external.feed.lifecycle.FeedManager;
import edu.uci.ics.asterix.external.feed.lifecycle.IFeedManager;
import edu.uci.ics.asterix.external.feed.lifecycle.IFeedMessage;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * Runtime for the @see{FeedMessageOperatorDescriptor}
 */
public class FeedMessageOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

    private final FeedId feedId;
    private final List<IFeedMessage> feedMessages;
    private IFeedManager feedManager;

    public FeedMessageOperatorNodePushable(IHyracksTaskContext ctx, FeedId feedId, List<IFeedMessage> feedMessages,
            boolean applyToAll, int partition, int nPartitions) {
        this.feedId = feedId;
        if (applyToAll) {
            this.feedMessages = feedMessages;
        } else {
            this.feedMessages = new ArrayList<IFeedMessage>();
            feedMessages.add(feedMessages.get(partition));
        }
        feedManager = (IFeedManager) FeedManager.INSTANCE;
    }

    @Override
    public void initialize() throws HyracksDataException {
        try {
            writer.open();
            for (IFeedMessage feedMessage : feedMessages) {
                feedManager.deliverMessage(feedId, feedMessage);
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            writer.close();
        }
    }

}
