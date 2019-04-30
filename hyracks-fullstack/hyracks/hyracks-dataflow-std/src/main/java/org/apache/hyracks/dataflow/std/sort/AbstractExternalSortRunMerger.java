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
package org.apache.hyracks.dataflow.std.sort;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.hyracks.api.comm.FrameConstants;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.sort.util.GroupVSizeFrame;
import org.apache.hyracks.util.annotations.CriticalPath;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractExternalSortRunMerger {

    protected final IHyracksTaskContext ctx;
    private final List<GeneratedRunFileReader> runs;
    private final BitSet currentGenerationRunAvailable;
    private final IBinaryComparator[] comparators;
    private final INormalizedKeyComputer nmkComputer;
    private final RecordDescriptor recordDesc;
    private final int maxMergeWidth;
    private final int topK;
    private List<GeneratedRunFileReader> partialRuns;
    private List<GroupVSizeFrame> inFrames;
    private VSizeFrame outputFrame;
    private boolean first;
    private static final Logger LOGGER = LogManager.getLogger();

    public AbstractExternalSortRunMerger(IHyracksTaskContext ctx, List<GeneratedRunFileReader> runs,
            IBinaryComparator[] comparators, INormalizedKeyComputer nmkComputer, RecordDescriptor recordDesc,
            int framesLimit) {
        this(ctx, runs, comparators, nmkComputer, recordDesc, framesLimit, Integer.MAX_VALUE);
    }

    AbstractExternalSortRunMerger(IHyracksTaskContext ctx, List<GeneratedRunFileReader> runs,
            IBinaryComparator[] comparators, INormalizedKeyComputer nmkComputer, RecordDescriptor recordDesc,
            int framesLimit, int topK) {
        this.ctx = ctx;
        this.runs = new LinkedList<>(runs);
        this.currentGenerationRunAvailable = new BitSet(runs.size());
        this.comparators = comparators;
        this.nmkComputer = nmkComputer;
        this.recordDesc = recordDesc;
        this.maxMergeWidth = framesLimit - 1;
        this.topK = topK;
        this.first = true;
    }

    @CriticalPath
    public void process(IFrameWriter finalWriter) throws HyracksDataException {
        try {
            createReusableObjects();
            int stop = runs.size();
            currentGenerationRunAvailable.set(0, stop);
            int numberOfPasses = 1;
            while (true) {
                int unUsed = selectPartialRuns(maxMergeWidth * ctx.getInitialFrameSize(), runs, partialRuns,
                        currentGenerationRunAvailable, stop);
                prepareFrames(unUsed, inFrames, partialRuns);

                if (!currentGenerationRunAvailable.isEmpty() || stop < runs.size()) {
                    GeneratedRunFileReader reader;
                    if (partialRuns.size() == 1) {
                        if (!currentGenerationRunAvailable.isEmpty()) {
                            throw new HyracksDataException("The record is too big to put into the merging frame, please"
                                    + " allocate more sorting memory");
                        } else {
                            reader = partialRuns.get(0);
                        }
                    } else {
                        RunFileWriter mergeFileWriter = prepareIntermediateMergeRunFile();
                        IFrameWriter mergeResultWriter = prepareIntermediateMergeResultWriter(mergeFileWriter);

                        try {
                            mergeResultWriter.open();
                            merge(mergeResultWriter, partialRuns);
                        } catch (Throwable t) {
                            mergeResultWriter.fail();
                            throw t;
                        } finally {
                            mergeResultWriter.close();
                        }
                        reader = mergeFileWriter.createReader();
                    }
                    runs.add(reader);

                    if (currentGenerationRunAvailable.isEmpty()) {
                        numberOfPasses++;
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("generated runs:" + stop);
                        }
                        runs.subList(0, stop).clear();
                        currentGenerationRunAvailable.clear();
                        currentGenerationRunAvailable.set(0, runs.size());
                        stop = runs.size();
                    }
                } else {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("final runs: {}", stop);
                        LOGGER.debug("number of passes: " + numberOfPasses);
                    }
                    merge(finalWriter, partialRuns);
                    break;
                }
            }
        } finally {
            for (RunFileReader reader : runs) {
                try {
                    reader.close(); // close is idempotent.
                } catch (Exception e) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.log(Level.WARN, e.getMessage(), e);
                    }
                }
            }
        }
    }

    private static int selectPartialRuns(int argBudget, List<GeneratedRunFileReader> runs,
            List<GeneratedRunFileReader> partialRuns, BitSet runAvailable, int stop) {
        partialRuns.clear();
        int budget = argBudget;
        int maxFrameSizeOfGenRun = 0;
        int nextRunId = runAvailable.nextSetBit(0);
        while (budget > 0 && nextRunId >= 0 && nextRunId < stop) {
            int runFrameSize = runs.get(nextRunId).getMaxFrameSize();
            if (budget - runFrameSize >= 0) {
                partialRuns.add(runs.get(nextRunId));
                budget -= runFrameSize;
                runAvailable.clear(nextRunId);
                maxFrameSizeOfGenRun = Math.max(runFrameSize, maxFrameSizeOfGenRun);
            }
            nextRunId = runAvailable.nextSetBit(nextRunId + 1);
        }
        return budget;
    }

    private void prepareFrames(int extraFreeMem, List<GroupVSizeFrame> inFrames,
            List<GeneratedRunFileReader> partialRuns) throws HyracksDataException {
        if (extraFreeMem > 0 && partialRuns.size() > 1) {
            int extraFrames = extraFreeMem / ctx.getInitialFrameSize();
            int avg = (extraFrames / partialRuns.size()) * ctx.getInitialFrameSize();
            int residue = extraFrames % partialRuns.size();
            for (int i = 0; i < residue; i++) {
                partialRuns.get(i).updateSize(Math.min(FrameConstants.MAX_FRAMESIZE,
                        partialRuns.get(i).getMaxFrameSize() + avg + ctx.getInitialFrameSize()));
            }
            for (int i = residue; i < partialRuns.size() && avg > 0; i++) {
                partialRuns.get(i)
                        .updateSize(Math.min(FrameConstants.MAX_FRAMESIZE, partialRuns.get(i).getMaxFrameSize() + avg));
            }
        }

        if (inFrames.size() > partialRuns.size()) {
            inFrames.subList(partialRuns.size(), inFrames.size()).clear();
        }
        int i;
        for (i = 0; i < inFrames.size(); i++) {
            inFrames.get(i).resize(partialRuns.get(i).getMaxFrameSize());
        }
        for (; i < partialRuns.size(); i++) {
            inFrames.add(new GroupVSizeFrame(ctx, partialRuns.get(i).getMaxFrameSize()));
        }
    }

    private void merge(IFrameWriter writer, List<GeneratedRunFileReader> partialRuns) throws HyracksDataException {
        RunMergingFrameReader merger = new RunMergingFrameReader(ctx, partialRuns, inFrames, getSortFields(),
                comparators, nmkComputer, recordDesc, topK);
        int io = 0;
        merger.open();
        try {
            while (merger.nextFrame(outputFrame)) {
                FrameUtils.flushFrame(outputFrame.getBuffer(), writer);
                io++;
            }
        } finally {
            merger.close();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Output " + io + " frames");
            }
        }
    }

    public void reset(List<GeneratedRunFileReader> newRuns) {
        this.runs.clear();
        this.runs.addAll(newRuns);
        this.currentGenerationRunAvailable.clear();
    }

    private void createReusableObjects() throws HyracksDataException {
        if (first) {
            first = false;
            inFrames = new ArrayList<>(maxMergeWidth);
            outputFrame = new VSizeFrame(ctx);
            partialRuns = new ArrayList<>(maxMergeWidth);
        }
    }

    public abstract IFrameWriter prepareSkipMergingFinalResultWriter(IFrameWriter nextWriter)
            throws HyracksDataException;

    protected abstract RunFileWriter prepareIntermediateMergeRunFile() throws HyracksDataException;

    protected abstract IFrameWriter prepareIntermediateMergeResultWriter(RunFileWriter mergeFileWriter)
            throws HyracksDataException;

    public abstract IFrameWriter prepareFinalMergeResultWriter(IFrameWriter nextWriter) throws HyracksDataException;

    protected abstract int[] getSortFields();

}
