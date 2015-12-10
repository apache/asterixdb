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
package org.apache.asterix.common.feeds;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.asterix.common.active.ActiveObjectId;
import org.apache.asterix.common.active.ActiveRuntime;
import org.apache.asterix.common.active.ActiveRuntimeId;
import org.apache.asterix.common.active.ActiveRuntimeInputHandler;
import org.apache.asterix.common.feeds.api.ISubscribableRuntime;
import org.apache.asterix.common.feeds.api.ISubscriberRuntime;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

public class SubscribableRuntime extends ActiveRuntime implements ISubscribableRuntime {

    protected static final Logger LOGGER = Logger.getLogger(SubscribableRuntime.class.getName());

    protected final ActiveObjectId feedId;
    protected final List<ISubscriberRuntime> subscribers;
    protected final RecordDescriptor recordDescriptor;
    protected final DistributeFeedFrameWriter dWriter;

    public SubscribableRuntime(ActiveObjectId feedId, ActiveRuntimeId runtimeId, ActiveRuntimeInputHandler inputHandler,
            DistributeFeedFrameWriter dWriter, RecordDescriptor recordDescriptor) {
        super(runtimeId, inputHandler, dWriter);
        this.feedId = feedId;
        this.recordDescriptor = recordDescriptor;
        this.dWriter = dWriter;
        this.subscribers = new ArrayList<ISubscriberRuntime>();
    }

    public ActiveObjectId getFeedId() {
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
        dWriter.unsubscribeFeed(collectionRuntime.getActiveFrameWriter());
        subscribers.remove(collectionRuntime);
    }

    @Override
    public synchronized List<ISubscriberRuntime> getSubscribers() {
        return subscribers;
    }

    @Override
    public DistributeFeedFrameWriter getActiveFrameWriter() {
        return dWriter;
    }

    public ActiveRuntimeType getFeedRuntimeType() {
        return runtimeId.getRuntimeType();
    }

    @Override
    public RecordDescriptor getRecordDescriptor() {
        return recordDescriptor;
    }

}
