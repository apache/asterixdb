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
package edu.uci.ics.hyracks.dataflow.hadoop.mapreduce;

import java.util.BitSet;

import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionCollector;
import edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IConnectorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractMToNConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.collectors.NonDeterministicChannelReader;
import edu.uci.ics.hyracks.dataflow.std.collectors.PartitionCollector;
import edu.uci.ics.hyracks.dataflow.std.connectors.PartitionDataWriter;

public class HashPartitioningShuffleConnectorDescriptor extends AbstractMToNConnectorDescriptor {
    private static final long serialVersionUID = 1L;

    private final MarshalledWritable<Configuration> mConfig;

    public HashPartitioningShuffleConnectorDescriptor(IConnectorDescriptorRegistry spec, MarshalledWritable<Configuration> mConfig) {
        super(spec);
        this.mConfig = mConfig;
    }

    @Override
    public IFrameWriter createPartitioner(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            IPartitionWriterFactory edwFactory, int index, int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        HadoopHelper helper = new HadoopHelper(mConfig);
        ITuplePartitionComputerFactory tpcf = helper.getTuplePartitionComputer();
        return new PartitionDataWriter(ctx, nConsumerPartitions, edwFactory, recordDesc, tpcf.createPartitioner());
    }

    @Override
    public IPartitionCollector createPartitionCollector(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            int receiverIndex, int nProducerPartitions, int nConsumerPartitions) throws HyracksDataException {
        BitSet expectedPartitions = new BitSet();
        expectedPartitions.set(0, nProducerPartitions);
        NonDeterministicChannelReader channelReader = new NonDeterministicChannelReader(nProducerPartitions,
                expectedPartitions);
        IFrameReader frameReader = new ShuffleFrameReader(ctx, channelReader, mConfig);
        return new PartitionCollector(ctx, getConnectorId(), receiverIndex, expectedPartitions, frameReader,
                channelReader);
    }
}