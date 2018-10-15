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
package org.apache.hyracks.dataflow.std.connectors;

import java.util.BitSet;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.IPartitionCollector;
import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractMToNConnectorDescriptor;
import org.apache.hyracks.dataflow.std.collectors.NonDeterministicChannelReader;
import org.apache.hyracks.dataflow.std.collectors.NonDeterministicFrameReader;
import org.apache.hyracks.dataflow.std.collectors.PartitionCollector;

public class LocalityAwareMToNPartitioningConnectorDescriptor extends AbstractMToNConnectorDescriptor {

    private static final long serialVersionUID = 1L;

    private ILocalityMap localityMap;

    private ITuplePartitionComputerFactory tpcf;

    public LocalityAwareMToNPartitioningConnectorDescriptor(IConnectorDescriptorRegistry spec,
            ITuplePartitionComputerFactory tpcf, ILocalityMap localityMap) {
        super(spec);
        this.localityMap = localityMap;
        this.tpcf = tpcf;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.hyracks.api.dataflow.IConnectorDescriptor#createPartitioner
     * (org.apache.hyracks.api.context.IHyracksTaskContext,
     * org.apache.hyracks.api.dataflow.value.RecordDescriptor,
     * org.apache.hyracks.api.comm.IPartitionWriterFactory, int, int, int)
     */
    @Override
    public IFrameWriter createPartitioner(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            IPartitionWriterFactory edwFactory, int index, int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        return new LocalityAwarePartitionDataWriter(ctx, edwFactory, recordDesc, tpcf.createPartitioner(ctx),
                nConsumerPartitions, localityMap, index);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hyracks.api.dataflow.IConnectorDescriptor#
     * createPartitionCollector
     * (org.apache.hyracks.api.context.IHyracksTaskContext,
     * org.apache.hyracks.api.dataflow.value.RecordDescriptor, int, int, int)
     */
    @Override
    public IPartitionCollector createPartitionCollector(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            int receiverIndex, int nProducerPartitions, int nConsumerPartitions) throws HyracksDataException {
        BitSet expectedPartitions = new BitSet(nProducerPartitions);
        for (int i = 0; i < nProducerPartitions; i++) {
            if (localityMap.isConnected(i, receiverIndex, nConsumerPartitions)) {
                expectedPartitions.set(i);
            }
        }
        NonDeterministicChannelReader channelReader =
                new NonDeterministicChannelReader(nProducerPartitions, expectedPartitions);
        NonDeterministicFrameReader frameReader = new NonDeterministicFrameReader(channelReader);
        return new PartitionCollector(ctx, getConnectorId(), receiverIndex, expectedPartitions, frameReader,
                channelReader);
    }
}
