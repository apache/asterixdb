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
import java.util.BitSet;
import java.util.Collection;

import org.apache.hyracks.api.channels.IInputChannel;
import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.comm.PartitionChannel;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.partitions.PartitionId;

public class PartitionCollector extends AbstractPartitionCollector {
    private final BitSet expectedPartitions;

    private final IFrameReader frameReader;

    private final IPartitionAcceptor pa;

    public PartitionCollector(IHyracksTaskContext ctx, ConnectorDescriptorId connectorId, int receiverIndex,
            BitSet expectedPartitions, IFrameReader frameReader, IPartitionAcceptor pa) {
        super(ctx, connectorId, receiverIndex);
        this.expectedPartitions = expectedPartitions;
        this.frameReader = frameReader;
        this.pa = pa;
    }

    @Override
    public void open() throws HyracksException {
    }

    @Override
    public void addPartitions(Collection<PartitionChannel> partitions) throws HyracksException {
        for (PartitionChannel pc : partitions) {
            PartitionId pid = pc.getPartitionId();
            IInputChannel channel = pc.getInputChannel();
            pa.addPartition(pid, channel);
            channel.open(ctx);
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
        Collection<PartitionId> c = new ArrayList<PartitionId>(expectedPartitions.cardinality());
        for (int i = expectedPartitions.nextSetBit(0); i >= 0; i = expectedPartitions.nextSetBit(i + 1)) {
            c.add(new PartitionId(getJobId(), getConnectorId(), i, getReceiverIndex()));
        }
        return c;
    }

    @Override
    public void abort() {

    }
}
