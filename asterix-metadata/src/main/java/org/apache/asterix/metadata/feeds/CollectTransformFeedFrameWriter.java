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

import java.nio.ByteBuffer;

import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedId;
import org.apache.asterix.common.feeds.api.IFeedOperatorOutputSideHandler;
import org.apache.asterix.common.feeds.api.ISubscribableRuntime;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;

public class CollectTransformFeedFrameWriter implements IFeedOperatorOutputSideHandler {

    private final FeedConnectionId connectionId;
    private IFrameWriter downstreamWriter;
    private final FrameTupleAccessor inputFrameTupleAccessor;
    private final FrameTupleAppender tupleAppender;
    private final IFrame frame;

    private ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);

    public CollectTransformFeedFrameWriter(IHyracksTaskContext ctx, IFrameWriter downstreamWriter,
            ISubscribableRuntime sourceRuntime, RecordDescriptor outputRecordDescriptor, FeedConnectionId connectionId)
            throws HyracksDataException {
        this.downstreamWriter = downstreamWriter;
        RecordDescriptor inputRecordDescriptor = sourceRuntime.getRecordDescriptor();
        inputFrameTupleAccessor = new FrameTupleAccessor(inputRecordDescriptor);
        tupleAppender = new FrameTupleAppender();
        frame = new VSizeFrame(ctx);
        tupleAppender.reset(frame, true);
        this.connectionId = connectionId;
    }

    @Override
    public void open() throws HyracksDataException {
        downstreamWriter.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        inputFrameTupleAccessor.reset(buffer);
        int nTuple = inputFrameTupleAccessor.getTupleCount();
        for (int t = 0; t < nTuple; t++) {
            tupleBuilder.addField(inputFrameTupleAccessor, t, 0);
            appendTupleToFrame();
            tupleBuilder.reset();
        }
    }

    private void appendTupleToFrame() throws HyracksDataException {
        if (!tupleAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                tupleBuilder.getSize())) {
            FrameUtils.flushFrame(frame.getBuffer(), downstreamWriter);
            tupleAppender.reset(frame, true);
            if (!tupleAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize())) {
                throw new IllegalStateException();
            }
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        downstreamWriter.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        downstreamWriter.close();
    }

    @Override
    public FeedId getFeedId() {
        return connectionId.getFeedId();
    }

    @Override
    public Type getType() {
        return Type.COLLECT_TRANSFORM_FEED_OUTPUT_HANDLER;
    }

    public IFrameWriter getDownstreamWriter() {
        return downstreamWriter;
    }

    public FeedConnectionId getConnectionId() {
        return connectionId;
    }

    public void reset(IFrameWriter writer) {
        this.downstreamWriter = writer;
    }

}