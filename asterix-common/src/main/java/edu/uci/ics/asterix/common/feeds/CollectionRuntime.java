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

import java.util.Map;

import edu.uci.ics.asterix.common.feeds.FeedFrameCollector.State;
import edu.uci.ics.asterix.common.feeds.api.ISubscribableRuntime;
import edu.uci.ics.asterix.common.feeds.api.ISubscriberRuntime;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

/**
 * Represents the feed runtime that collects feed tuples from another feed.
 * In case of a primary feed, the CollectionRuntime collects tuples from the feed
 * intake job. For a secondary feed, tuples are collected from the intake/compute
 * runtime associated with the source feed.
 */
public class CollectionRuntime extends FeedRuntime implements ISubscriberRuntime {

    private final FeedConnectionId connectionId;
    private final ISubscribableRuntime sourceRuntime;
    private final Map<String, String> feedPolicy;
    private FeedFrameCollector frameCollector;

    public CollectionRuntime(FeedConnectionId connectionId, FeedRuntimeId runtimeId,
            FeedRuntimeInputHandler inputSideHandler, IFrameWriter outputSideWriter,
            ISubscribableRuntime sourceRuntime, Map<String, String> feedPolicy) {
        super(runtimeId, inputSideHandler, outputSideWriter);
        this.connectionId = connectionId;
        this.sourceRuntime = sourceRuntime;
        this.feedPolicy = feedPolicy;
    }

    public State waitTillCollectionOver() throws InterruptedException {
        if (!(isCollectionOver())) {
            synchronized (frameCollector) {
                while (!isCollectionOver()) {
                    frameCollector.wait();
                }
            }
        }
        return frameCollector.getState();
    }

    private boolean isCollectionOver() {
        return frameCollector.getState().equals(FeedFrameCollector.State.FINISHED)
                || frameCollector.getState().equals(FeedFrameCollector.State.HANDOVER);
    }

    public void setMode(Mode mode) {
        getInputHandler().setMode(mode);
    }

    @Override
    public Map<String, String> getFeedPolicy() {
        return feedPolicy;
    }

    public FeedConnectionId getConnectionId() {
        return connectionId;
    }

    public ISubscribableRuntime getSourceRuntime() {
        return sourceRuntime;
    }

    public void setFrameCollector(FeedFrameCollector frameCollector) {
        this.frameCollector = frameCollector;
    }

    public FeedFrameCollector getFrameCollector() {
        return frameCollector;
    }

}
