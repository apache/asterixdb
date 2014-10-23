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
package edu.uci.ics.hyracks.dataflow.std.group.sort;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.preclustered.PreclusteredGroupWriter;
import edu.uci.ics.hyracks.dataflow.std.sort.IFrameSorter;
import edu.uci.ics.hyracks.dataflow.std.sort.RunMergingFrameReader;

/**
 * Group-by aggregation is pushed into multi-pass merge of external sort.
 * 
 * @author yingyib
 */
public class ExternalSortGroupByRunMerger {

    private final IHyracksTaskContext ctx;
    private final List<IFrameReader> runs;
    private final RecordDescriptor inputRecordDesc;
    private final RecordDescriptor partialAggRecordDesc;
    private final RecordDescriptor outRecordDesc;
    private final int framesLimit;
    private final IFrameWriter writer;
    private List<ByteBuffer> inFrames;
    private ByteBuffer outFrame;
    private FrameTupleAppender outFrameAppender;

    private final IFrameSorter frameSorter; // Used in External sort, no replacement
    // selection

    private final int[] groupFields;
    private final INormalizedKeyComputer firstKeyNkc;
    private final IBinaryComparator[] comparators;
    private final IAggregatorDescriptorFactory mergeAggregatorFactory;
    private final IAggregatorDescriptorFactory partialAggregatorFactory;
    private final boolean localSide;

    private final int[] mergeSortFields;
    private final int[] mergeGroupFields;
    private final IBinaryComparator[] groupByComparators;

    // Constructor for external sort, no replacement selection
    public ExternalSortGroupByRunMerger(IHyracksTaskContext ctx, IFrameSorter frameSorter, List<IFrameReader> runs,
            int[] sortFields, RecordDescriptor inRecordDesc, RecordDescriptor partialAggRecordDesc,
            RecordDescriptor outRecordDesc, int framesLimit, IFrameWriter writer, int[] groupFields,
            INormalizedKeyComputer nmk, IBinaryComparator[] comparators,
            IAggregatorDescriptorFactory partialAggregatorFactory, IAggregatorDescriptorFactory aggregatorFactory,
            boolean localStage) {
        this.ctx = ctx;
        this.frameSorter = frameSorter;
        this.runs = new LinkedList<IFrameReader>(runs);
        this.inputRecordDesc = inRecordDesc;
        this.partialAggRecordDesc = partialAggRecordDesc;
        this.outRecordDesc = outRecordDesc;
        this.framesLimit = framesLimit;
        this.writer = writer;

        this.groupFields = groupFields;
        this.firstKeyNkc = nmk;
        this.comparators = comparators;
        this.mergeAggregatorFactory = aggregatorFactory;
        this.partialAggregatorFactory = partialAggregatorFactory;
        this.localSide = localStage;

        //create merge sort fields
        int numSortFields = sortFields.length;
        mergeSortFields = new int[numSortFields];
        for (int i = 0; i < numSortFields; i++) {
            mergeSortFields[i] = i;
        }

        //create merge group fields
        int numGroupFields = groupFields.length;
        mergeGroupFields = new int[numGroupFields];
        for (int i = 0; i < numGroupFields; i++) {
            mergeGroupFields[i] = i;
        }

        //setup comparators for grouping
        groupByComparators = new IBinaryComparator[Math.min(mergeGroupFields.length, comparators.length)];
        for (int i = 0; i < groupByComparators.length; i++) {
            groupByComparators[i] = comparators[i];
        }
    }

    public void process() throws HyracksDataException {
        IAggregatorDescriptorFactory aggregatorFactory = localSide ? partialAggregatorFactory : mergeAggregatorFactory;
        PreclusteredGroupWriter pgw = new PreclusteredGroupWriter(ctx, groupFields, groupByComparators,
                aggregatorFactory, inputRecordDesc, outRecordDesc, writer, false);
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
                        FileReference newRun = ctx.createManagedWorkspaceFile(ExternalSortGroupByRunMerger.class
                                .getSimpleName());
                        IFrameWriter mergeResultWriter = new RunFileWriter(newRun, ctx.getIOManager());

                        aggregatorFactory = localSide ? mergeAggregatorFactory : partialAggregatorFactory;
                        pgw = new PreclusteredGroupWriter(ctx, mergeGroupFields, groupByComparators, aggregatorFactory,
                                partialAggRecordDesc, partialAggRecordDesc, mergeResultWriter, true);
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
                    pgw = new PreclusteredGroupWriter(ctx, mergeGroupFields, groupByComparators,
                            mergeAggregatorFactory, partialAggRecordDesc, outRecordDesc, writer, false);
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
        } finally {
            pgw.close();
        }
    }

    private void merge(IFrameWriter mergeResultWriter, IFrameReader[] runCursors) throws HyracksDataException {
        RunMergingFrameReader merger = new RunMergingFrameReader(ctx, runCursors, inFrames, mergeSortFields,
                comparators, firstKeyNkc, partialAggRecordDesc);
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
