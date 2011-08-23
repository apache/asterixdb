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
package edu.uci.ics.hyracks.dataflow.std.collectors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import edu.uci.ics.hyracks.api.channels.IInputChannel;
import edu.uci.ics.hyracks.api.channels.IInputChannelMonitor;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.PartitionChannel;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.sort.RunMergingFrameReader;

public class SortMergePartitionCollector extends AbstractPartitionCollector {
    private final int[] sortFields;

    private final IBinaryComparator[] comparators;

    private final RecordDescriptor recordDescriptor;

    private final int maxConcurrentMerges;

    private final IInputChannel[] channels;

    private final int nSenders;

    private final boolean stable;

    private final FrameReader frameReader;

    private final PartitionBatchManager pbm;

    public SortMergePartitionCollector(IHyracksTaskContext ctx, ConnectorDescriptorId connectorId, int receiverIndex,
            int[] sortFields, IBinaryComparator[] comparators, RecordDescriptor recordDescriptor,
            int maxConcurrentMerges, int nSenders, boolean stable) {
        super(ctx, connectorId, receiverIndex);
        this.sortFields = sortFields;
        this.comparators = comparators;
        this.recordDescriptor = recordDescriptor;
        this.maxConcurrentMerges = maxConcurrentMerges;
        channels = new IInputChannel[nSenders];
        this.nSenders = nSenders;
        this.stable = stable;
        this.frameReader = new FrameReader();
        pbm = new NonDeterministicPartitionBatchManager();
    }

    @Override
    public void open() throws HyracksException {
    }

    @Override
    public void addPartitions(Collection<PartitionChannel> partitions) throws HyracksException {
        for (PartitionChannel pc : partitions) {
            PartitionId pid = pc.getPartitionId();
            IInputChannel channel = pc.getInputChannel();
            InputChannelFrameReader channelReader = new InputChannelFrameReader(channel);
            channel.registerMonitor(channelReader);
            channel.setAttachment(channelReader);
            int senderIndex = pid.getSenderIndex();
            synchronized (this) {
                channels[senderIndex] = channel;
            }
            pbm.addPartition(senderIndex);
            channel.open();
        }
    }

    @Override
    public IFrameReader getReader() throws HyracksException {
        return frameReader;
    }

    @Override
    public void close() throws HyracksException {

    }

    @Override
    public Collection<PartitionId> getRequiredPartitionIds() throws HyracksException {
        Collection<PartitionId> requiredPartitionIds = new ArrayList<PartitionId>();
        for (int i = 0; i < nSenders; ++i) {
            requiredPartitionIds.add(new PartitionId(getJobId(), getConnectorId(), i, receiverIndex));
        }
        return requiredPartitionIds;
    }

    @Override
    public void abort() {

    }

    private abstract class PartitionBatchManager {
        protected abstract void addPartition(int index);

        protected abstract void getNextBatch(List<IFrameReader> batch, int size) throws HyracksDataException;
    }

    private class NonDeterministicPartitionBatchManager extends PartitionBatchManager {
        private List<IFrameReader> partitions;

        private List<IFrameReader> batch;

        private int requiredSize;

        public NonDeterministicPartitionBatchManager() {
            partitions = new ArrayList<IFrameReader>();
        }

        @Override
        protected void addPartition(int index) {
            synchronized (SortMergePartitionCollector.this) {
                if (batch != null && batch.size() < requiredSize) {
                    batch.add((IFrameReader) channels[index].getAttachment());
                    if (batch.size() == requiredSize) {
                        SortMergePartitionCollector.this.notifyAll();
                    }
                } else {
                    partitions.add((IFrameReader) channels[index].getAttachment());
                }
            }
        }

        @Override
        protected void getNextBatch(List<IFrameReader> batch, int size) throws HyracksDataException {
            synchronized (SortMergePartitionCollector.this) {
                if (partitions.size() <= size) {
                    batch.addAll(partitions);
                    partitions.clear();
                } else if (partitions.size() > size) {
                    List<IFrameReader> sublist = partitions.subList(0, size);
                    batch.addAll(sublist);
                    sublist.clear();
                }
                if (batch.size() == size) {
                    return;
                }
                this.batch = batch;
                this.requiredSize = size;
                while (batch.size() < size) {
                    try {
                        SortMergePartitionCollector.this.wait();
                    } catch (InterruptedException e) {
                        throw new HyracksDataException(e);
                    }
                }
                this.batch = null;
            }
        }
    }

    private static class InputChannelFrameReader implements IFrameReader, IInputChannelMonitor {
        private final IInputChannel channel;

        private int availableFrames;

        private boolean eos;

        private boolean failed;

        public InputChannelFrameReader(IInputChannel channel) {
            this.channel = channel;
            availableFrames = 0;
            eos = false;
            failed = false;
        }

        @Override
        public void open() throws HyracksDataException {
        }

        @Override
        public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
            synchronized (this) {
                while (!failed && !eos && availableFrames <= 0) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        throw new HyracksDataException(e);
                    }
                }
                if (failed) {
                    throw new HyracksDataException("Failure occurred on input");
                }
                if (availableFrames <= 0 && eos) {
                    return false;
                }
                --availableFrames;
            }
            ByteBuffer srcBuffer = channel.getNextBuffer();
            FrameUtils.copy(srcBuffer, buffer);
            channel.recycleBuffer(srcBuffer);
            return true;
        }

        @Override
        public void close() throws HyracksDataException {

        }

        @Override
        public synchronized void notifyFailure(IInputChannel channel) {
            failed = true;
            notifyAll();
        }

        @Override
        public synchronized void notifyDataAvailability(IInputChannel channel, int nFrames) {
            availableFrames += nFrames;
            notifyAll();
        }

        @Override
        public synchronized void notifyEndOfStream(IInputChannel channel) {
            eos = true;
            notifyAll();
        }
    }

    private class FrameReader implements IFrameReader {
        private RunMergingFrameReader merger;

        @Override
        public void open() throws HyracksDataException {
            if (maxConcurrentMerges >= nSenders) {
                List<ByteBuffer> inFrames = new ArrayList<ByteBuffer>();
                for (int i = 0; i < nSenders; ++i) {
                    inFrames.add(ByteBuffer.allocate(ctx.getFrameSize()));
                }
                List<IFrameReader> batch = new ArrayList<IFrameReader>();
                pbm.getNextBatch(batch, nSenders);
                merger = new RunMergingFrameReader(ctx, batch.toArray(new IFrameReader[nSenders]), inFrames,
                        sortFields, comparators, recordDescriptor);
            } else {
                // multi level merge.
                throw new HyracksDataException("Not yet supported");
            }
            merger.open();
        }

        @Override
        public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
            buffer.position(buffer.capacity());
            buffer.limit(buffer.capacity());
            return merger.nextFrame(buffer);
        }

        @Override
        public void close() throws HyracksDataException {
            merger.close();
        }
    }
}