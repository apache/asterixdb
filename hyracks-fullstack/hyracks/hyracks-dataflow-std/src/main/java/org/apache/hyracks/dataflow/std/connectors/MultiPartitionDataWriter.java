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

import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITupleMultiPartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class MultiPartitionDataWriter extends AbstractPartitionDataWriter {

    private final ITupleMultiPartitionComputer tpc;

    public MultiPartitionDataWriter(IHyracksTaskContext ctx, int consumerPartitionCount,
            IPartitionWriterFactory pwFactory, RecordDescriptor recordDescriptor, ITupleMultiPartitionComputer tpc)
            throws HyracksDataException {
        super(ctx, consumerPartitionCount, pwFactory, recordDescriptor);
        this.tpc = tpc;
    }

    @Override
    public void open() throws HyracksDataException {
        super.open();
        tpc.initialize();
    }

    @Override
    protected void processTuple(int tupleIndex) throws HyracksDataException {
        BitSet partitionSet = tpc.partition(tupleAccessor, tupleIndex, consumerPartitionCount);
        for (int p = partitionSet.nextSetBit(0); p >= 0; p = partitionSet.nextSetBit(p + 1)) {
            appendToPartitionWriter(tupleIndex, p);
        }
    }
}
