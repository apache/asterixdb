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
package org.apache.hyracks.control.nc.partitions;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.partitions.IPartition;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.control.common.job.PartitionState;

public class PipelinedPartition implements IFrameWriter, IPartition {
    private final IHyracksTaskContext ctx;

    private final PartitionManager manager;

    private final PartitionId pid;

    private final TaskAttemptId taId;

    private IFrameWriter delegate;

    private volatile boolean pendingConnection = true;

    public PipelinedPartition(IHyracksTaskContext ctx, PartitionManager manager, PartitionId pid, TaskAttemptId taId) {
        this.ctx = ctx;
        this.manager = manager;
        this.pid = pid;
        this.taId = taId;
    }

    @Override
    public IHyracksTaskContext getTaskContext() {
        return ctx;
    }

    @Override
    public boolean isReusable() {
        return false;
    }

    @Override
    public void deallocate() {
        // do nothing
    }

    @Override
    public synchronized void writeTo(IFrameWriter writer) {
        delegate = writer;
        notifyAll();
    }

    @Override
    public void open() throws HyracksDataException {
        manager.registerPartition(pid, ctx.getJobletContext().getJobId().getCcId(), taId, this, PartitionState.STARTED,
                false);
        pendingConnection = true;
        ensureConnected();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        delegate.nextFrame(buffer);
    }

    private void ensureConnected() throws HyracksDataException {
        if (pendingConnection) {
            synchronized (this) {
                while (delegate == null) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw HyracksDataException.create(e);
                    }
                }
            }
            delegate.open();
        }
        pendingConnection = false;
    }

    @Override
    public void fail() throws HyracksDataException {
        if (!pendingConnection) {
            delegate.fail();
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (!pendingConnection) {
            delegate.close();
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        if (!pendingConnection) {
            delegate.flush();
        }
    }
}
