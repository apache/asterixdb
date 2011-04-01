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
package edu.uci.ics.hyracks.dataflow.std.connectors;

import java.nio.ByteBuffer;
import java.util.BitSet;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionCollector;
import edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.NonDeterministicPartitionCollector;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractMToNConnectorDescriptor;

public class MToNReplicatingConnectorDescriptor extends AbstractMToNConnectorDescriptor {
    public MToNReplicatingConnectorDescriptor(JobSpecification spec) {
        super(spec);
    }

    private static final long serialVersionUID = 1L;

    @Override
    public IFrameWriter createPartitioner(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            IPartitionWriterFactory edwFactory, int index, int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        final IFrameWriter[] epWriters = new IFrameWriter[nConsumerPartitions];
        for (int i = 0; i < nConsumerPartitions; ++i) {
            epWriters[i] = edwFactory.createFrameWriter(i);
        }
        return new IFrameWriter() {
            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                buffer.mark();
                for (int i = 0; i < epWriters.length; ++i) {
                    if (i != 0) {
                        buffer.reset();
                    }
                    epWriters[i].nextFrame(buffer);
                }
            }

            @Override
            public void close() throws HyracksDataException {
                for (int i = 0; i < epWriters.length; ++i) {
                    epWriters[i].close();
                }
            }

            @Override
            public void open() throws HyracksDataException {
                for (int i = 0; i < epWriters.length; ++i) {
                    epWriters[i].open();
                }
            }

            @Override
            public void flush() throws HyracksDataException {
                for (int i = 0; i < epWriters.length; ++i) {
                    epWriters[i].flush();
                }
            }
        };
    }

    @Override
    public IPartitionCollector createPartitionCollector(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            int index, int nProducerPartitions, int nConsumerPartitions) throws HyracksDataException {
        BitSet expectedPartitions = new BitSet(nProducerPartitions);
        expectedPartitions.set(0, nProducerPartitions);
        return new NonDeterministicPartitionCollector(ctx, getConnectorId(), index, expectedPartitions);
    }
}