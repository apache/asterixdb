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

import java.nio.ByteBuffer;

import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FeedFrameCollector implements IFrameWriter {

    private final FeedConnectionId connectionId;
    private IFrameWriter writer;
    private State state;

    public enum State {
        ACTIVE,
        FINISHED,
        TRANSITION,
        HANDOVER
    }

    public FeedFrameCollector(FeedPolicyAccessor feedPolicyAccessor, IFrameWriter writer,
            FeedConnectionId connectionId) {
        this.connectionId = connectionId;
        this.writer = writer;
        this.state = State.ACTIVE;
    }

    @Override
    public synchronized void close() throws HyracksDataException {
        writer.close();
        state = State.FINISHED;
        notify();
    }

    public synchronized void disconnect() {
        setState(State.FINISHED);
    }

    @Override
    public synchronized void nextFrame(ByteBuffer frame) throws HyracksDataException {
        writer.nextFrame(frame);
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
    }

    public IFrameWriter getFrameWriter() {
        return writer;
    }

    public void setFrameWriter(IFrameWriter writer) {
        this.writer = writer;
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

    @Override
    public synchronized void flush() throws HyracksDataException {
        writer.flush();
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    public FeedConnectionId getConnectionId() {
        return connectionId;
    }
}
