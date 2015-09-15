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

import java.nio.ByteBuffer;
import java.util.logging.Level;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.api.IMessageReceiver;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FeedFrameCollector extends MessageReceiver<DataBucket> implements IMessageReceiver<DataBucket> {

    private final FeedConnectionId connectionId;
    private final FrameDistributor frameDistributor;
    private FeedPolicyAccessor fpa;
    private IFrameWriter frameWriter;
    private State state;

    public enum State {
        ACTIVE,
        FINISHED,
        TRANSITION,
        HANDOVER
    }

    public FeedFrameCollector(FrameDistributor frameDistributor, FeedPolicyAccessor feedPolicyAccessor,
            IFrameWriter frameWriter, FeedConnectionId connectionId) {
        super();
        this.frameDistributor = frameDistributor;
        this.fpa = feedPolicyAccessor;
        this.connectionId = connectionId;
        this.frameWriter = frameWriter;
        this.state = State.ACTIVE;
    }

    @Override
    public void processMessage(DataBucket bucket) throws Exception {
        try {
            ByteBuffer frame = bucket.getContent();
            switch (bucket.getContentType()) {
                case DATA:
                    frameWriter.nextFrame(frame);
                    break;
                case EOD:
                    closeCollector();
                    break;
                case EOSD:
                    throw new AsterixException("Received data bucket with content of type " + bucket.getContentType());
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to process data bucket " + bucket + ", encountered exception " + e.getMessage());
            }
        } finally {
            bucket.doneReading();
        }
    }

    public void closeCollector() {
        if (state.equals(State.TRANSITION)) {
            super.close(true);
            setState(State.ACTIVE);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(this + " is now " + State.ACTIVE + " mode, processing frames synchronously");
            }
        } else {
            flushPendingMessages();
            setState(State.FINISHED);
            synchronized (frameDistributor.getRegisteredCollectors()) {
                frameDistributor.getRegisteredCollectors().notifyAll();
            }
            disconnect();
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Closed collector " + this);
        }
    }

    public synchronized void disconnect() {
        setState(State.FINISHED);
    }

    public synchronized void nextFrame(ByteBuffer frame) throws HyracksDataException {
        frameWriter.nextFrame(frame);
    }

    public FeedPolicyAccessor getFeedPolicyAccessor() {
        return fpa;
    }

    public synchronized State getState() {
        return state;
    }

    public synchronized void setState(State state) {
        this.state = state;
        switch (state) {
            case FINISHED:
            case HANDOVER:
                notifyAll();
                break;
            default:
                break;
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Frame Collector " + this.frameDistributor.getFeedRuntimeType() + " switched to " + state);
        }
    }

    public IFrameWriter getFrameWriter() {
        return frameWriter;
    }

    public void setFrameWriter(IFrameWriter frameWriter) {
        this.frameWriter = frameWriter;
    }

    @Override
    public String toString() {
        return "FrameCollector " + connectionId + "," + state + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof FeedFrameCollector) {
            return connectionId.equals(((FeedFrameCollector) o).connectionId);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return connectionId.toString().hashCode();
    }

}
