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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.api.ISubscribableRuntime;
import edu.uci.ics.asterix.common.feeds.api.ISubscriberRuntime;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class SubscribableRuntime extends FeedRuntime implements ISubscribableRuntime {

    protected static final Logger LOGGER = Logger.getLogger(SubscribableRuntime.class.getName());

    protected final FeedId feedId;
    protected final List<ISubscriberRuntime> subscribers;
    protected final RecordDescriptor recordDescriptor;
    protected final DistributeFeedFrameWriter dWriter;

    public SubscribableRuntime(FeedId feedId, FeedRuntimeId runtimeId, FeedRuntimeInputHandler inputHandler,
            DistributeFeedFrameWriter dWriter, RecordDescriptor recordDescriptor) {
        super(runtimeId, inputHandler, dWriter);
        this.feedId = feedId;
        this.recordDescriptor = recordDescriptor;
        this.dWriter = dWriter;
        this.subscribers = new ArrayList<ISubscriberRuntime>();
    }

    public FeedId getFeedId() {
        return feedId;
    }

    @Override
    public String toString() {
        return "SubscribableRuntime" + " [" + feedId + "]" + "(" + runtimeId + ")";
    }

    @Override
    public synchronized void subscribeFeed(FeedPolicyAccessor fpa, CollectionRuntime collectionRuntime)
            throws Exception {
        FeedFrameCollector collector = dWriter.subscribeFeed(new FeedPolicyAccessor(collectionRuntime.getFeedPolicy()),
                collectionRuntime.getInputHandler(), collectionRuntime.getConnectionId());
        collectionRuntime.setFrameCollector(collector);
        subscribers.add(collectionRuntime);
    }

    @Override
    public synchronized void unsubscribeFeed(CollectionRuntime collectionRuntime) throws Exception {
        dWriter.unsubscribeFeed(collectionRuntime.getFeedFrameWriter());
        subscribers.remove(collectionRuntime);
    }

    @Override
    public synchronized List<ISubscriberRuntime> getSubscribers() {
        return subscribers;
    }

    @Override
    public DistributeFeedFrameWriter getFeedFrameWriter() {
        return dWriter;
    }

    public FeedRuntimeType getFeedRuntimeType() {
        return runtimeId.getFeedRuntimeType();
    }

    @Override
    public RecordDescriptor getRecordDescriptor() {
        return recordDescriptor;
    }

}
