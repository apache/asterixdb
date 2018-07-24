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
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.api.channels.IInputChannel;
import org.apache.hyracks.api.channels.IInputChannelMonitor;
import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.comm.IPartitionCollector;
import org.apache.hyracks.api.comm.PartitionChannel;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.partitions.PartitionId;

public class ReceiveSideMaterializingCollector implements IPartitionCollector {
    private final IHyracksTaskContext ctx;

    private PartitionManager manager;

    private final IPartitionCollector delegate;

    private final TaskAttemptId taId;

    private final Executor executor;

    public ReceiveSideMaterializingCollector(IHyracksTaskContext ctx, PartitionManager manager,
            IPartitionCollector collector, TaskAttemptId taId, Executor executor) {
        this.ctx = ctx;
        this.manager = manager;
        this.delegate = collector;
        this.taId = taId;
        this.executor = executor;
    }

    @Override
    public JobId getJobId() {
        return delegate.getJobId();
    }

    @Override
    public ConnectorDescriptorId getConnectorId() {
        return delegate.getConnectorId();
    }

    @Override
    public int getReceiverIndex() {
        return delegate.getReceiverIndex();
    }

    @Override
    public void open() throws HyracksException {
        delegate.open();
    }

    @Override
    public void addPartitions(Collection<PartitionChannel> partitions) throws HyracksException {
        for (final PartitionChannel pc : partitions) {
            PartitionWriter writer = new PartitionWriter(pc);
            executor.execute(writer);
        }
    }

    private class PartitionWriter implements Runnable, IInputChannelMonitor {
        private PartitionChannel pc;

        private final AtomicInteger nAvailableFrames;

        private final AtomicBoolean eos;

        private final AtomicBoolean failed;

        public PartitionWriter(PartitionChannel pc) {
            this.pc = pc;
            nAvailableFrames = new AtomicInteger(0);
            eos = new AtomicBoolean(false);
            failed = new AtomicBoolean(false);
        }

        @Override
        public synchronized void notifyFailure(IInputChannel channel, int errorCode) {
            failed.set(true);
            notifyAll();
        }

        @Override
        public synchronized void notifyDataAvailability(IInputChannel channel, int nFrames) {
            nAvailableFrames.addAndGet(nFrames);
            notifyAll();
        }

        @Override
        public synchronized void notifyEndOfStream(IInputChannel channel) {
            eos.set(true);
            notifyAll();
        }

        @Override
        public void run() {
            PartitionId pid = pc.getPartitionId();
            MaterializedPartitionWriter mpw = new MaterializedPartitionWriter(ctx, manager, pid, taId, executor);
            IInputChannel channel = pc.getInputChannel();
            try {
                channel.registerMonitor(this);
                channel.open(ctx);
                mpw.open();
                while (true) {
                    if (nAvailableFrames.get() > 0) {
                        ByteBuffer buffer = channel.getNextBuffer();
                        nAvailableFrames.decrementAndGet();
                        mpw.nextFrame(buffer);
                        channel.recycleBuffer(buffer);
                    } else if (eos.get()) {
                        break;
                    } else if (failed.get()) {
                        // Sends failure notification to its downstream.
                        // It's not supposed to throw exception here because it is on the failure notification channel.
                        mpw.fail();
                    } else {
                        try {
                            synchronized (this) {
                                if (nAvailableFrames.get() <= 0 && !eos.get() && !failed.get()) {
                                    wait();
                                }
                            }
                        } catch (InterruptedException e) {
                            throw HyracksDataException.create(e);
                        }
                    }
                }
                mpw.close();
                channel.close();
                delegate.addPartitions(Collections
                        .singleton(new PartitionChannel(pid, new MaterializedPartitionInputChannel(1, pid, manager))));
            } catch (HyracksException e) {
            }
        }
    }

    @Override
    public IFrameReader getReader() throws HyracksException {
        return delegate.getReader();
    }

    @Override
    public void close() throws HyracksException {
        delegate.close();
    }

    @Override
    public Collection<PartitionId> getRequiredPartitionIds() throws HyracksException {
        return delegate.getRequiredPartitionIds();
    }

    @Override
    public void abort() {
        delegate.abort();
    }
}
