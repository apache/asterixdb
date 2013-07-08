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
package edu.uci.ics.hyracks.dataflow.std.collectors;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.channels.IInputChannel;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.partitions.PartitionId;

public class NonDeterministicPartitionBatchManager implements IPartitionBatchManager {
    private final IInputChannel[] channels;

    private List<IFrameReader> partitions;

    private List<IFrameReader> batch;

    private int requiredSize;

    public NonDeterministicPartitionBatchManager(int nSenders) {
        channels = new IInputChannel[nSenders];
        partitions = new ArrayList<IFrameReader>();
    }

    @Override
    public synchronized void addPartition(PartitionId pid, IInputChannel channel) {
        channels[pid.getSenderIndex()] = channel;
        InputChannelFrameReader channelReader = new InputChannelFrameReader(channel);
        channel.registerMonitor(channelReader);
        if (batch != null && batch.size() < requiredSize) {
            batch.add(channelReader);
            if (batch.size() == requiredSize) {
                notifyAll();
            }
        } else {
            partitions.add(channelReader);
        }
    }

    @Override
    public synchronized void getNextBatch(List<IFrameReader> batch, int size) throws HyracksDataException {
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
                wait();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }
        this.batch = null;
    }
}