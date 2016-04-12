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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITupleRangePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.storage.IGrowableIntArray;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.storage.common.arraylist.IntArrayList;

public class PartitionRangeDataWriter extends AbstractPartitionDataWriter {
    private final ITupleRangePartitionComputer tpc;
    private final IGrowableIntArray map;

    public PartitionRangeDataWriter(IHyracksTaskContext ctx, int consumerPartitionCount,
            IPartitionWriterFactory pwFactory, RecordDescriptor recordDescriptor, ITupleRangePartitionComputer tpc)
                    throws HyracksDataException {
        super(ctx, consumerPartitionCount, pwFactory, recordDescriptor);
        this.tpc = tpc;
        this.map = new IntArrayList(8, 8);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        if (!allocatedFrame) {
            allocateFrames();
            allocatedFrame = true;
        }
        tupleAccessor.reset(buffer);
        int tupleCount = tupleAccessor.getTupleCount();
        for (int i = 0; i < tupleCount; ++i) {
            tpc.partition(tupleAccessor, i, consumerPartitionCount, map);
            for (int h = 0; h < map.size(); ++h) {
                FrameUtils.appendToWriter(pWriters[map.get(h)], appenders[map.get(h)], tupleAccessor, i);
            }
            map.clear();
        }
    }
}
