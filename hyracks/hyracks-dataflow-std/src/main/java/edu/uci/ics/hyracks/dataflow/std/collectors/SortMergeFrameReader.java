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
package edu.uci.ics.hyracks.dataflow.std.collectors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.sort.RunMergingFrameReader;

public class SortMergeFrameReader implements IFrameReader {
    private IHyracksTaskContext ctx;
    private final int maxConcurrentMerges;
    private final int nSenders;
    private final int[] sortFields;
    private final IBinaryComparator[] comparators;
    private final INormalizedKeyComputer nmkComputer;
    private final RecordDescriptor recordDescriptor;
    private final IPartitionBatchManager pbm;

    private RunMergingFrameReader merger;

    public SortMergeFrameReader(IHyracksTaskContext ctx, int maxConcurrentMerges, int nSenders, int[] sortFields,
            IBinaryComparator[] comparators, INormalizedKeyComputer nmkComputer, RecordDescriptor recordDescriptor,
            IPartitionBatchManager pbm) {
        this.ctx = ctx;
        this.maxConcurrentMerges = maxConcurrentMerges;
        this.nSenders = nSenders;
        this.sortFields = sortFields;
        this.comparators = comparators;
        this.nmkComputer = nmkComputer;
        this.recordDescriptor = recordDescriptor;
        this.pbm = pbm;
    }

    @Override
    public void open() throws HyracksDataException {
        if (maxConcurrentMerges >= nSenders) {
            List<ByteBuffer> inFrames = new ArrayList<ByteBuffer>();
            for (int i = 0; i < nSenders; ++i) {
                inFrames.add(ByteBuffer.allocate(ctx.getFrameSize()));
            }
            List<IFrameReader> batch = new ArrayList<IFrameReader>();
            pbm.getNextBatch(batch, nSenders);
            merger = new RunMergingFrameReader(ctx, batch.toArray(new IFrameReader[nSenders]), inFrames, sortFields,
                    comparators, nmkComputer, recordDescriptor);
        } else {
            // multi level merge.
            throw new HyracksDataException("Not yet supported");
        }
        merger.open();
    }

    @Override
    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
        buffer.position(buffer.capacity());
        buffer.limit(buffer.capacity());
        return merger.nextFrame(buffer);
    }

    @Override
    public void close() throws HyracksDataException {
        merger.close();
    }
}