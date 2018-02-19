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

package org.apache.hyracks.dataflow.std.buffermanager;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Variable size frame managed by a buffer manager. The frame size can be changed.
 * The caller of this class needs to ensure that this class releases its buffer by calling close() method.
 */
public class BufferManagerBackedVSizeFrame implements IFrame {

    protected final int minFrameSize;
    protected final IHyracksFrameMgrContext ctx;
    protected final ISimpleFrameBufferManager bufferManager;
    protected ByteBuffer frame;

    public BufferManagerBackedVSizeFrame(IHyracksFrameMgrContext ctx, ISimpleFrameBufferManager bufferManager)
            throws HyracksDataException {
        this(ctx, ctx.getInitialFrameSize(), bufferManager);
    }

    public BufferManagerBackedVSizeFrame(IHyracksFrameMgrContext ctx, int frameSize,
            ISimpleFrameBufferManager bufferManager) throws HyracksDataException {
        this.ctx = ctx;
        this.minFrameSize = ctx.getInitialFrameSize();
        this.bufferManager = bufferManager;
        frame = bufferManager.acquireFrame(frameSize);
    }

    @Override
    public ByteBuffer getBuffer() {
        return frame;
    }

    @Override
    public void ensureFrameSize(int newSize) throws HyracksDataException {
        if (newSize > getFrameSize()) {
            bufferManager.releaseFrame(frame);
            frame = bufferManager.acquireFrame(newSize);
        }
    }

    @Override
    public void resize(int frameSize) throws HyracksDataException {
        if (getFrameSize() != frameSize) {
            bufferManager.releaseFrame(frame);
            frame = bufferManager.acquireFrame(frameSize);
        }
    }

    @Override
    public int getFrameSize() {
        return frame.capacity();
    }

    @Override
    public int getMinSize() {
        return minFrameSize;
    }

    @Override
    public void reset() throws HyracksDataException {
        resize(minFrameSize);
        Arrays.fill(frame.array(), (byte) 0);
        frame.clear();
    }

    /**
     * Allocates an initial frame and clears it.
     */
    public void acquireFrame() throws HyracksDataException {
        if (frame == null) {
            frame = bufferManager.acquireFrame(minFrameSize);
        }
        reset();
    }

    public void destroy() throws HyracksDataException {
        if (frame != null) {
            bufferManager.releaseFrame(frame);
            frame = null;
        }
    }

}
