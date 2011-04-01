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
package edu.uci.ics.hyracks.dataflow.common.comm;

import java.util.Collection;

import edu.uci.ics.hyracks.api.channels.IInputChannel;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.PartitionChannel;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;

public class SortMergePartitionCollector extends AbstractPartitionCollector {
    private final FrameTuplePairComparator tpc;

    private final FrameTupleAppender appender;

    private final RecordDescriptor recordDescriptor;

    private final int maxFramesLimit;

    private IInputChannel[] channels;

    public SortMergePartitionCollector(IHyracksTaskContext ctx, ConnectorDescriptorId connectorId, int receiverIndex,
            int[] sortFields, IBinaryComparator[] comparators, RecordDescriptor recordDescriptor, int maxFramesLimit,
            int nSenders) {
        super(ctx, connectorId, receiverIndex);
        tpc = new FrameTuplePairComparator(sortFields, sortFields, comparators);
        appender = new FrameTupleAppender(ctx.getFrameSize());
        this.recordDescriptor = recordDescriptor;
        this.maxFramesLimit = maxFramesLimit;
        channels = new IInputChannel[nSenders];
    }

    @Override
    public void open() throws HyracksException {
    }

    @Override
    public void addPartitions(Collection<PartitionChannel> partitions) throws HyracksException {

    }

    @Override
    public IFrameReader getReader() throws HyracksException {
        return null;
    }

    @Override
    public void close() throws HyracksException {

    }

    @Override
    public Collection<PartitionId> getRequiredPartitionIds() throws HyracksException {
        return null;
    }

    @Override
    public void abort() {

    }
}