/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executor;

import edu.uci.ics.hyracks.api.channels.IInputChannel;
import edu.uci.ics.hyracks.api.channels.IInputChannelMonitor;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IPartitionCollector;
import edu.uci.ics.hyracks.api.comm.PartitionChannel;
import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.partitions.PartitionId;

public class ReceiveSideMaterializingCollector implements IPartitionCollector {
    private final IHyracksRootContext ctx;

    private PartitionManager manager;

    private final IPartitionCollector delegate;

    private final TaskAttemptId taId;

    private final Executor executor;

    public ReceiveSideMaterializingCollector(IHyracksRootContext ctx, PartitionManager manager,
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

        private int nAvailableFrames;

        private boolean eos;

        public PartitionWriter(PartitionChannel pc) {
            this.pc = pc;
            nAvailableFrames = 0;
            eos = false;
        }

        @Override
        public synchronized void notifyDataAvailability(IInputChannel channel, int nFrames) {
            nAvailableFrames += nFrames;
            notifyAll();
        }

        @Override
        public synchronized void notifyEndOfStream(IInputChannel channel) {
            eos = true;
            notifyAll();
        }

        @Override
        public synchronized void run() {
            PartitionId pid = pc.getPartitionId();
            MaterializedPartitionWriter mpw = new MaterializedPartitionWriter(ctx, manager, pid, taId, executor);
            IInputChannel channel = pc.getInputChannel();
            try {
                channel.registerMonitor(this);
                channel.open();
                mpw.open();
                while (true) {
                    if (nAvailableFrames > 0) {
                        ByteBuffer buffer = channel.getNextBuffer();
                        --nAvailableFrames;
                        mpw.nextFrame(buffer);
                        channel.recycleBuffer(buffer);
                    } else if (eos) {
                        break;
                    } else {
                        try {
                            wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                mpw.close();
                channel.close();
                delegate.addPartitions(Collections.singleton(new PartitionChannel(pid,
                        new MaterializedPartitionInputChannel(ctx, 5, pid, manager))));
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