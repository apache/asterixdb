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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.pregelix.dataflow.std.group.ClusteredGroupWriter;
import edu.uci.ics.pregelix.dataflow.std.group.IClusteredAggregatorDescriptorFactory;

/**
 * Group-by aggregation is pushed into multi-pass merge of external sort.
 * 
 * @author yingyib
 */
public class ExternalSortRunMerger {

    private final IHyracksTaskContext ctx;
    private final List<IFrameReader> runs;
    private final int[] sortFields;
    private final RecordDescriptor inRecordDesc;
    private final RecordDescriptor outRecordDesc;
    private final int framesLimit;
    private final IFrameWriter writer;
    private List<ByteBuffer> inFrames;
    private ByteBuffer outFrame;
    private FrameTupleAppender outFrameAppender;

    private IFrameSorter frameSorter; // Used in External sort, no replacement
                                      // selection

    private final int[] groupFields;
    private final IBinaryComparator[] comparators;
    private final IClusteredAggregatorDescriptorFactory aggregatorFactory;
    private final IClusteredAggregatorDescriptorFactory partialAggregatorFactory;
    private final boolean localSide;

    // Constructor for external sort, no replacement selection
    public ExternalSortRunMerger(IHyracksTaskContext ctx, IFrameSorter frameSorter, List<IFrameReader> runs,
            int[] sortFields, RecordDescriptor inRecordDesc, RecordDescriptor outRecordDesc, int framesLimit,
            IFrameWriter writer, int[] groupFields, IBinaryComparator[] comparators,
            IClusteredAggregatorDescriptorFactory partialAggregatorFactory,
            IClusteredAggregatorDescriptorFactory aggregatorFactory, boolean localSide) {
        this.ctx = ctx;
        this.frameSorter = frameSorter;
        this.runs = new LinkedList<IFrameReader>(runs);
        this.sortFields = sortFields;
        this.inRecordDesc = inRecordDesc;
        this.outRecordDesc = outRecordDesc;
        this.framesLimit = framesLimit;
        this.writer = writer;

        this.groupFields = groupFields;
        this.comparators = comparators;
        this.aggregatorFactory = aggregatorFactory;
        this.partialAggregatorFactory = partialAggregatorFactory;
        this.localSide = localSide;
    }

    public void process() throws HyracksDataException {
        ClusteredGroupWriter pgw = new ClusteredGroupWriter(ctx, groupFields, comparators,
                localSide ? partialAggregatorFactory : aggregatorFactory, inRecordDesc, outRecordDesc, writer);
        try {
            if (runs.size() <= 0) {
                pgw.open();
                if (frameSorter != null && frameSorter.getFrameCount() > 0) {
                    frameSorter.flushFrames(pgw);
                }
                /** recycle sort buffer */
                frameSorter.close();
            } else {
                /** recycle sort buffer */
                frameSorter.close();

                inFrames = new ArrayList<ByteBuffer>();
                outFrame = ctx.allocateFrame();
                outFrameAppender = new FrameTupleAppender(ctx.getFrameSize());
                outFrameAppender.reset(outFrame, true);
                for (int i = 0; i < framesLimit - 1; ++i) {
                    inFrames.add(ctx.allocateFrame());
                }
                int maxMergeWidth = framesLimit - 1;
                while (runs.size() > maxMergeWidth) {
                    int generationSeparator = 0;
                    while (generationSeparator < runs.size() && runs.size() > maxMergeWidth) {
                        int mergeWidth = Math.min(Math.min(runs.size() - generationSeparator, maxMergeWidth),
                                runs.size() - maxMergeWidth + 1);
                        FileReference newRun = ctx.createManagedWorkspaceFile(ExternalSortRunMerger.class
                                .getSimpleName());
                        IFrameWriter mergeResultWriter = new RunFileWriter(newRun, ctx.getIOManager());
                        pgw = new ClusteredGroupWriter(ctx, groupFields, comparators, partialAggregatorFactory,
                                inRecordDesc, inRecordDesc, mergeResultWriter);
                        pgw.open();
                        IFrameReader[] runCursors = new RunFileReader[mergeWidth];
                        for (int i = 0; i < mergeWidth; i++) {
                            runCursors[i] = runs.get(generationSeparator + i);
                        }
                        merge(pgw, runCursors);
                        pgw.close();
                        runs.subList(generationSeparator, mergeWidth + generationSeparator).clear();
                        runs.add(generationSeparator++, ((RunFileWriter) mergeResultWriter).createReader());
                    }
                }
                if (!runs.isEmpty()) {
                    pgw = new ClusteredGroupWriter(ctx, groupFields, comparators, aggregatorFactory, inRecordDesc,
                            inRecordDesc, writer);
                    pgw.open();
                    IFrameReader[] runCursors = new RunFileReader[runs.size()];
                    for (int i = 0; i < runCursors.length; i++) {
                        runCursors[i] = runs.get(i);
                    }
                    merge(pgw, runCursors);
                }
            }
        } catch (Exception e) {
            pgw.fail();
            throw new HyracksDataException(e);
        } finally {
            pgw.close();
        }
    }

    private void merge(IFrameWriter mergeResultWriter, IFrameReader[] runCursors) throws HyracksDataException {
        RunMergingFrameReader merger = new RunMergingFrameReader(ctx, runCursors, inFrames, sortFields, inRecordDesc);
        merger.open();
        try {
            while (merger.nextFrame(outFrame)) {
                FrameUtils.flushFrame(outFrame, mergeResultWriter);
            }
        } finally {
            merger.close();
        }
    }
}
