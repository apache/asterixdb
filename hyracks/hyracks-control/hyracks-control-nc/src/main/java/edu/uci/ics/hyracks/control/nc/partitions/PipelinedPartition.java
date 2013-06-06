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
package edu.uci.ics.hyracks.control.nc.partitions;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.partitions.IPartition;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.common.job.PartitionState;

public class PipelinedPartition implements IFrameWriter, IPartition {
    private final IHyracksTaskContext ctx;

    private final PartitionManager manager;

    private final PartitionId pid;

    private final TaskAttemptId taId;

    private IFrameWriter delegate;

    private boolean pendingConnection;

    private boolean failed;

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
        manager.registerPartition(pid, taId, this, PartitionState.STARTED);
        failed = false;
        pendingConnection = true;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        ensureConnected();
        delegate.nextFrame(buffer);
    }

    private void ensureConnected() throws HyracksDataException {
        if (pendingConnection) {
            synchronized (this) {
                while (delegate == null) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }
            delegate.open();
        }
        pendingConnection = false;
    }

    @Override
    public void fail() throws HyracksDataException {
        failed = true;
        if (delegate != null) {
            delegate.fail();
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (!failed) {
            ensureConnected();
            manager.updatePartitionState(pid, taId, this, PartitionState.COMMITTED);
            delegate.close();
        }
    }
}