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
package edu.uci.ics.pregelix.dataflow.std.sort;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.pregelix.dataflow.std.group.ClusteredGroupWriter;
import edu.uci.ics.pregelix.dataflow.std.group.IClusteredAggregatorDescriptorFactory;

public class ExternalSortRunGenerator implements IFrameWriter {
    private final IHyracksTaskContext ctx;
    private final IFrameSorter frameSorter;
    private final List<IFrameReader> runs;
    private final int maxSortFrames;

    private final int[] groupFields;
    private final IBinaryComparator[] comparators;
    private final IClusteredAggregatorDescriptorFactory aggregatorFactory;
    private final RecordDescriptor inRecordDesc;
    private final RecordDescriptor outRecordDesc;

    public ExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields, RecordDescriptor recordDesc,
            int framesLimit, int[] groupFields, IBinaryComparator[] comparators,
            IClusteredAggregatorDescriptorFactory aggregatorFactory, RecordDescriptor outRecordDesc)
            throws HyracksDataException {
        this.ctx = ctx;
        this.frameSorter = new FrameSorterQuickSort(ctx, sortFields, recordDesc);
        this.runs = new LinkedList<IFrameReader>();
        this.maxSortFrames = framesLimit - 1;

        this.groupFields = groupFields;
        this.comparators = comparators;
        this.aggregatorFactory = aggregatorFactory;
        this.inRecordDesc = recordDesc;
        this.outRecordDesc = outRecordDesc;
    }

    @Override
    public void open() throws HyracksDataException {
        runs.clear();
        frameSorter.reset();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        if (frameSorter.getFrameCount() >= maxSortFrames) {
            flushFramesToRun();
        }
        frameSorter.insertFrame(buffer);
    }

    @Override
    public void close() throws HyracksDataException {
        if (frameSorter.getFrameCount() > 0) {
            if (runs.size() <= 0) {
                frameSorter.sortFrames();
            } else {
                flushFramesToRun();
            }
        }
    }

    private void flushFramesToRun() throws HyracksDataException {
        frameSorter.sortFrames();
        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                ExternalSortRunGenerator.class.getSimpleName());
        RunFileWriter writer = new RunFileWriter(file, ctx.getIOManager());
        ClusteredGroupWriter pgw = new ClusteredGroupWriter(ctx, groupFields, comparators, aggregatorFactory,
                this.inRecordDesc, this.outRecordDesc, writer);
        pgw.open();

        try {
            frameSorter.flushFrames(pgw);
        } finally {
            pgw.close();
        }
        frameSorter.reset();
        runs.add(writer.createReader());
    }

    @Override
    public void fail() throws HyracksDataException {
    }

    public IFrameSorter getFrameSorter() {
        return frameSorter;
    }

    public List<IFrameReader> getRuns() {
        return runs;
    }
}