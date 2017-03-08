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

package org.apache.hyracks.api.comm;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Variable size frame. The buffer inside can be resized.
 */
public class VSizeFrame implements IFrame {

    protected final int minFrameSize;
    protected IHyracksFrameMgrContext ctx;
    protected ByteBuffer buffer;

    public VSizeFrame(IHyracksFrameMgrContext ctx) throws HyracksDataException {
        this(ctx, ctx.getInitialFrameSize());
    }

    public VSizeFrame(IHyracksFrameMgrContext ctx, int frameSize) throws HyracksDataException {
        this.minFrameSize = ctx.getInitialFrameSize();
        this.ctx = ctx;
        buffer = ctx.allocateFrame(frameSize);
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public void ensureFrameSize(int newSize) throws HyracksDataException {
        if (newSize > getFrameSize()) {
            buffer = ctx.reallocateFrame(buffer, newSize, true);
        }
    }

    @Override
    public void resize(int frameSize) throws HyracksDataException {
        if (getFrameSize() != frameSize) {
            buffer = ctx.reallocateFrame(buffer, frameSize, false);
        }
    }

    @Override
    public int getFrameSize() {
        return buffer.capacity();
    }

    @Override
    public int getMinSize() {
        return minFrameSize;
    }

    @Override
    public void reset() throws HyracksDataException {
        resize(minFrameSize);
        buffer.clear();
    }

}
