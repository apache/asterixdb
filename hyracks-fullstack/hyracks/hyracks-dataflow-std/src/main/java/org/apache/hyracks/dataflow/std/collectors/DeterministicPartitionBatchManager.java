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
package org.apache.hyracks.dataflow.std.collectors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.api.channels.IInputChannel;
import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.partitions.PartitionId;

public class DeterministicPartitionBatchManager implements IPartitionBatchManager {
    private final IFrameReader[] partitions;
    private List<IFrameReader> partitionsList;

    public DeterministicPartitionBatchManager(int nSenders) {
        this.partitions = new IFrameReader[nSenders];
    }

    @Override
    public synchronized void addPartition(PartitionId partitionId, IInputChannel channel) {
        InputChannelFrameReader channelReader = new InputChannelFrameReader(channel);
        channel.registerMonitor(channelReader);
        partitions[partitionId.getSenderIndex()] = channelReader;
        if (allPartitionsAdded()) {
            partitionsList = new ArrayList<>(Arrays.asList(partitions));
            notifyAll();
        }
    }

    @Override
    public synchronized void getNextBatch(List<IFrameReader> batch, int requestedSize) throws HyracksDataException {
        while (!allPartitionsAdded()) {
            try {
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw HyracksDataException.create(e);
            }
        }
        if (partitionsList.isEmpty()) {
            return;
        }
        if (requestedSize >= partitionsList.size()) {
            batch.addAll(partitionsList);
            partitionsList.clear();
        } else {
            List<IFrameReader> subBatch = partitionsList.subList(0, requestedSize);
            batch.addAll(subBatch);
            subBatch.clear();
        }
    }

    private boolean allPartitionsAdded() {
        for (int i = 0; i < partitions.length; i++) {
            if (partitions[i] == null) {
                return false;
            }
        }
        return true;
    }
}
