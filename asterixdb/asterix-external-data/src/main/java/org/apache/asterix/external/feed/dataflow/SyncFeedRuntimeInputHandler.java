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

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class SyncFeedRuntimeInputHandler extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    private final FeedExceptionHandler exceptionHandler;

    public SyncFeedRuntimeInputHandler(IHyracksTaskContext ctx, IFrameWriter writer, FrameTupleAccessor fta) {
        this.writer = writer;
        this.exceptionHandler = new FeedExceptionHandler(ctx, fta);
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
    }

    @Override
    public void nextFrame(ByteBuffer frame) throws HyracksDataException {
        while (frame != null) {
            try {
                writer.nextFrame(frame);
                return;
            } catch (HyracksDataException e) {
                frame = exceptionHandler.handle(e, frame);
                if (frame == null) {
                    throw e;
                }
            }
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        writer.close();
    }

    @Override
    public void flush() throws HyracksDataException {
        writer.flush();
    }

}
