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
package edu.uci.ics.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;

public class ExternalSortRunGenerator implements IFrameWriter {
    private final IHyracksTaskContext ctx;
    private final IFrameSorter frameSorter;
    private final List<IFrameReader> runs;
    private final int maxSortFrames;

    public ExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, Algorithm alg, int framesLimit) throws HyracksDataException {
        this.ctx = ctx;
        if (alg == Algorithm.MERGE_SORT) {
            frameSorter = new FrameSorterMergeSort(ctx, sortFields, firstKeyNormalizerFactory, comparatorFactories,
                    recordDesc);
        } else {
            frameSorter = new FrameSorterQuickSort(ctx, sortFields, firstKeyNormalizerFactory, comparatorFactories,
                    recordDesc);
        }
        runs = new LinkedList<IFrameReader>();
        maxSortFrames = framesLimit - 1;
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
        writer.open();
        try {
            frameSorter.flushFrames(writer);
        } finally {
            writer.close();
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