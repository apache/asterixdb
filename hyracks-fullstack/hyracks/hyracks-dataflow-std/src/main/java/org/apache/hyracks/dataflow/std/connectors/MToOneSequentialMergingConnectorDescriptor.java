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

import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.IPartitionCollector;
import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.data.partition.OnePartitionComputerFactory;
import org.apache.hyracks.dataflow.std.base.AbstractMToNConnectorDescriptor;
import org.apache.hyracks.dataflow.std.collectors.DeterministicPartitionBatchManager;
import org.apache.hyracks.dataflow.std.collectors.IPartitionBatchManager;
import org.apache.hyracks.dataflow.std.collectors.PartitionCollector;
import org.apache.hyracks.dataflow.std.collectors.SequentialMergeFrameReader;

public class MToOneSequentialMergingConnectorDescriptor extends AbstractMToNConnectorDescriptor {
    private static final long serialVersionUID = 1L;
    private final ITuplePartitionComputerFactory tpcf;

    public MToOneSequentialMergingConnectorDescriptor(IConnectorDescriptorRegistry spec) {
        super(spec);
        tpcf = new OnePartitionComputerFactory();
    }

    @Override
    public IFrameWriter createPartitioner(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            IPartitionWriterFactory edwFactory, int index, int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        // TODO(ali): create a single partition data writer instead
        return new PartitionDataWriter(ctx, nConsumerPartitions, edwFactory, recordDesc, tpcf.createPartitioner(ctx));
    }

    @Override
    public IPartitionCollector createPartitionCollector(IHyracksTaskContext ctx, RecordDescriptor recordDesc, int index,
            int nProducerPartitions, int nConsumerPartitions) throws HyracksDataException {
        IPartitionBatchManager pbm = new DeterministicPartitionBatchManager(nProducerPartitions);
        IFrameReader sequentialMergeReader = new SequentialMergeFrameReader(nProducerPartitions, pbm);
        BitSet expectedPartitions = new BitSet();
        expectedPartitions.set(0, nProducerPartitions);
        return new PartitionCollector(ctx, getConnectorId(), index, expectedPartitions, sequentialMergeReader, pbm);
    }
}
