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
package org.apache.asterix.external.feed.dataflow;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.external.feed.api.IFeedRuntime.FeedRuntimeType;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.management.FeedId;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

/**
 * Provides mechanism for distributing the frames, as received from an operator to a
 * set of registered readers. Each reader typically operates at a different pace. Readers
 * are isolated from each other to ensure that a slow reader does not impact the progress of
 * others.
 **/
public class DistributeFeedFrameWriter implements IFrameWriter {

    /** A unique identifier for the feed to which the incoming tuples belong. **/
    private final FeedId feedId;

    /**
     * An instance of FrameDistributor that provides the mechanism for distributing a frame to multiple readers, each
     * operating in isolation.
     **/
    private final FrameDistributor frameDistributor;

    /** The original frame writer instantiated as part of job creation **/
    private final IFrameWriter writer;

    /** The feed operation whose output is being distributed by the DistributeFeedFrameWriter **/
    private final FeedRuntimeType feedRuntimeType;

    /** The value of the partition 'i' if this is the i'th instance of the associated operator **/
    private final int partition;

    public DistributeFeedFrameWriter(IHyracksTaskContext ctx, FeedId feedId, IFrameWriter writer,
            FeedRuntimeType feedRuntimeType, int partition, FrameTupleAccessor fta) throws IOException {
        this.feedId = feedId;
        this.frameDistributor = new FrameDistributor();
        this.feedRuntimeType = feedRuntimeType;
        this.partition = partition;
        this.writer = writer;
    }

    /**
     * @param fpa
     *            Feed policy accessor
     * @param nextOnlyWriter
     *            the writer which will deliver the buffers
     * @param connectionId
     *            (Dataverse - Dataset - Feed)
     * @return A frame collector.
     * @throws HyracksDataException
     */
    public void subscribe(FeedFrameCollector collector) throws HyracksDataException {
        frameDistributor.registerFrameCollector(collector);
    }

    public void unsubscribeFeed(FeedConnectionId connectionId) throws HyracksDataException {
        frameDistributor.deregisterFrameCollector(connectionId);
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            frameDistributor.close();
        } finally {
            writer.close();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void nextFrame(ByteBuffer frame) throws HyracksDataException {
        frameDistributor.nextFrame(frame);
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
    }

    @Override
    public String toString() {
        return feedId.toString() + feedRuntimeType + "[" + partition + "]";
    }

    @Override
    public void flush() throws HyracksDataException {
        frameDistributor.flush();
    }
}
