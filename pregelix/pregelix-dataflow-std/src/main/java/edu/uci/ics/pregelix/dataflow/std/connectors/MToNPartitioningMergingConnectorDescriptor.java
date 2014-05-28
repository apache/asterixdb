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
package edu.uci.ics.pregelix.dataflow.std.connectors;

import java.util.BitSet;

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
import edu.uci.ics.hyracks.dataflow.std.collectors.IPartitionBatchManager;
import edu.uci.ics.hyracks.dataflow.std.collectors.NonDeterministicPartitionBatchManager;
import edu.uci.ics.hyracks.dataflow.std.collectors.PartitionCollector;
import edu.uci.ics.hyracks.dataflow.std.connectors.PartitionDataWriter;
import edu.uci.ics.pregelix.dataflow.std.collectors.SortMergeFrameReader;

public class MToNPartitioningMergingConnectorDescriptor extends AbstractMToNConnectorDescriptor {
    private static final long serialVersionUID = 1L;

    private final ITuplePartitionComputerFactory tpcf;
    private final int[] sortFields;

    public MToNPartitioningMergingConnectorDescriptor(IConnectorDescriptorRegistry spec,
            ITuplePartitionComputerFactory tpcf, int[] sortFields) {
        this(spec, tpcf, sortFields, false);
    }

    public MToNPartitioningMergingConnectorDescriptor(IConnectorDescriptorRegistry spec,
            ITuplePartitionComputerFactory tpcf, int[] sortFields, boolean stable) {
        super(spec);
        this.tpcf = tpcf;
        this.sortFields = sortFields;
    }

    @Override
    public IFrameWriter createPartitioner(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            IPartitionWriterFactory edwFactory, int index, int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        final PartitionDataWriter hashWriter = new PartitionDataWriter(ctx, nConsumerPartitions, edwFactory,
                recordDesc, tpcf.createPartitioner());
        return hashWriter;
    }

    @Override
    public IPartitionCollector createPartitionCollector(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            int index, int nProducerPartitions, int nConsumerPartitions) throws HyracksDataException {
        IPartitionBatchManager pbm = new NonDeterministicPartitionBatchManager(nProducerPartitions);
        IFrameReader sortMergeFrameReader = new SortMergeFrameReader(ctx, nProducerPartitions, nProducerPartitions,
                sortFields, recordDesc, pbm);
        BitSet expectedPartitions = new BitSet();
        expectedPartitions.set(0, nProducerPartitions);
        return new PartitionCollector(ctx, getConnectorId(), index, expectedPartitions, sortMergeFrameReader, pbm);
    }
}