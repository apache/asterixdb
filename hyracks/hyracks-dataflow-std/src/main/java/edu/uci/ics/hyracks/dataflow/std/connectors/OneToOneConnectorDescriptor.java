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
package edu.uci.ics.hyracks.dataflow.std.connectors;

import java.util.BitSet;

import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionCollector;
import edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory;
import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.IConstraintAcceptor;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionCountExpression;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.ActivityCluster;
import edu.uci.ics.hyracks.api.job.IConnectorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.collectors.NonDeterministicChannelReader;
import edu.uci.ics.hyracks.dataflow.std.collectors.NonDeterministicFrameReader;
import edu.uci.ics.hyracks.dataflow.std.collectors.PartitionCollector;

public class OneToOneConnectorDescriptor extends AbstractConnectorDescriptor {
    private static final long serialVersionUID = 1L;

    public OneToOneConnectorDescriptor(IConnectorDescriptorRegistry spec) {
        super(spec);
    }

    @Override
    public IFrameWriter createPartitioner(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            IPartitionWriterFactory edwFactory, int index, int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        return edwFactory.createFrameWriter(index);
    }

    @Override
    public IPartitionCollector createPartitionCollector(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            int index, int nProducerPartitions, int nConsumerPartitions) throws HyracksDataException {
        BitSet expectedPartitions = new BitSet(nProducerPartitions);
        expectedPartitions.set(index);
        NonDeterministicChannelReader channelReader = new NonDeterministicChannelReader(nProducerPartitions,
                expectedPartitions);
        NonDeterministicFrameReader frameReader = new NonDeterministicFrameReader(channelReader);
        return new PartitionCollector(ctx, getConnectorId(), index, expectedPartitions, frameReader, channelReader);
    }

    @Override
    public void contributeSchedulingConstraints(IConstraintAcceptor constraintAcceptor, ActivityCluster ac,
            ICCApplicationContext appCtx) {
        OperatorDescriptorId consumer = ac.getConsumerActivity(getConnectorId()).getOperatorDescriptorId();
        OperatorDescriptorId producer = ac.getProducerActivity(getConnectorId()).getOperatorDescriptorId();

        constraintAcceptor.addConstraint(new Constraint(new PartitionCountExpression(consumer),
                new PartitionCountExpression(producer)));
    }

    @Override
    public void indicateTargetPartitions(int nProducerPartitions, int nConsumerPartitions, int producerIndex,
            BitSet targetBitmap) {
        targetBitmap.clear();
        targetBitmap.set(producerIndex);
    }

    @Override
    public void indicateSourcePartitions(int nProducerPartitions, int nConsumerPartitions, int consumerIndex,
            BitSet sourceBitmap) {
        sourceBitmap.clear();
        sourceBitmap.set(consumerIndex);
    }

    @Override
    public boolean allProducersToAllConsumers() {
        return false;
    }
}