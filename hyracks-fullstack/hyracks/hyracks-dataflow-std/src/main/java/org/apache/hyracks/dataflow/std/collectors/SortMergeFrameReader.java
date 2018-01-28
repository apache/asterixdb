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
import java.util.List;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.sort.RunMergingFrameReader;

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
            List<IFrame> inFrames = new ArrayList<>(nSenders);
            for (int i = 0; i < nSenders; ++i) {
                inFrames.add(new VSizeFrame(ctx));
            }
            List<IFrameReader> batch = new ArrayList<IFrameReader>(nSenders);
            pbm.getNextBatch(batch, nSenders);
            merger = new RunMergingFrameReader(ctx, batch, inFrames, sortFields, comparators, nmkComputer,
                    recordDescriptor);
        } else {
            // multi level merge.
            throw new HyracksDataException("Not yet supported");
        }
        merger.open();
    }

    @Override
    public boolean nextFrame(IFrame frame) throws HyracksDataException {
        return merger.nextFrame(frame);
    }

    @Override
    public void close() throws HyracksDataException {
        merger.close();
    }
}
