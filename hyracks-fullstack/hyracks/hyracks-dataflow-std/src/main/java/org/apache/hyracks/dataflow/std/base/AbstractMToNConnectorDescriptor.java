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
package org.apache.hyracks.dataflow.std.base;

import java.util.BitSet;

import org.apache.hyracks.api.comm.IPartitionCollector;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.collectors.NonDeterministicChannelReader;
import org.apache.hyracks.dataflow.std.collectors.NonDeterministicFrameReader;
import org.apache.hyracks.dataflow.std.collectors.PartitionCollector;

public abstract class AbstractMToNConnectorDescriptor extends AbstractConnectorDescriptor {
    private static final long serialVersionUID = 1L;

    public AbstractMToNConnectorDescriptor(IConnectorDescriptorRegistry spec) {
        super(spec);
    }

    @Override
    public void indicateTargetPartitions(int nProducerPartitions, int nConsumerPartitions, int producerIndex,
            BitSet targetBitmap) {
        targetBitmap.clear();
        targetBitmap.set(0, nConsumerPartitions);
    }

    @Override
    public void indicateSourcePartitions(int nProducerPartitions, int nConsumerPartitions, int consumerIndex,
            BitSet sourceBitmap) {
        sourceBitmap.clear();
        sourceBitmap.set(0, nProducerPartitions);
    }

    @Override
    public boolean allProducersToAllConsumers() {
        return true;
    }

    @Override
    public IPartitionCollector createPartitionCollector(IHyracksTaskContext ctx, RecordDescriptor recordDesc, int index,
            int nProducerPartitions, int nConsumerPartitions) throws HyracksDataException {
        BitSet expectedPartitions = new BitSet(nProducerPartitions);
        expectedPartitions.set(0, nProducerPartitions);
        NonDeterministicChannelReader channelReader =
                new NonDeterministicChannelReader(nProducerPartitions, expectedPartitions);
        NonDeterministicFrameReader frameReader = new NonDeterministicFrameReader(channelReader);
        return new PartitionCollector(ctx, getConnectorId(), index, expectedPartitions, frameReader, channelReader);
    }
}
