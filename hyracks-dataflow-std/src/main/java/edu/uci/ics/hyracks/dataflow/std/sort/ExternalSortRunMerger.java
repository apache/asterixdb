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
package edu.uci.ics.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;
import java.util.ArrayList;
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

public class ExternalSortRunMerger {
    private final IHyracksTaskContext ctx;
    private final FrameSorter frameSorter;
    private final List<IFrameReader> runs;
    private final int[] sortFields;
    private final IBinaryComparator[] comparators;
    private final RecordDescriptor recordDesc;
    private final int framesLimit;
    private final IFrameWriter writer;
    private List<ByteBuffer> inFrames;
    private ByteBuffer outFrame;
    private FrameTupleAppender outFrameAppender;

    public ExternalSortRunMerger(IHyracksTaskContext ctx, FrameSorter frameSorter, List<IFrameReader> runs,
            int[] sortFields, IBinaryComparator[] comparators, RecordDescriptor recordDesc, int framesLimit,
            IFrameWriter writer) {
        this.ctx = ctx;
        this.frameSorter = frameSorter;
        this.runs = runs;
        this.sortFields = sortFields;
        this.comparators = comparators;
        this.recordDesc = recordDesc;
        this.framesLimit = framesLimit;
        this.writer = writer;
    }

    public void process() throws HyracksDataException {
        writer.open();
        try {
            if (runs.size() <= 0) {
                if (frameSorter != null && frameSorter.getFrameCount() > 0) {
                    frameSorter.flushFrames(writer);
                }
            } else {
                inFrames = new ArrayList<ByteBuffer>();
                outFrame = ctx.allocateFrame();
                outFrameAppender = new FrameTupleAppender(ctx.getFrameSize());
                outFrameAppender.reset(outFrame, true);
                for (int i = 0; i < framesLimit - 1; ++i) {
                    inFrames.add(ctx.allocateFrame());
                }
                while (runs.size() > 0) {
                    try {
                        doPass(runs);
                    } catch (Exception e) {
                        throw new HyracksDataException(e);
                    }
                }
            }
        } finally {
            writer.close();
        }
    }

    // creates a new run from runs that can fit in memory.
    private void doPass(List<IFrameReader> runs) throws HyracksDataException {
        FileReference newRun = null;
        IFrameWriter writer = this.writer;
        boolean finalPass = false;
        if (runs.size() + 1 <= framesLimit) { // + 1 outFrame
            finalPass = true;
            for (int i = inFrames.size() - 1; i >= runs.size(); i--) {
                inFrames.remove(i);
            }
        } else {
            newRun = ctx.createManagedWorkspaceFile(ExternalSortRunMerger.class.getSimpleName());
            writer = new RunFileWriter(newRun, ctx.getIOManager());
            writer.open();
        }
        try {
            IFrameReader[] runCursors = new RunFileReader[inFrames.size()];
            for (int i = 0; i < inFrames.size(); i++) {
                runCursors[i] = runs.get(i);
            }
            RunMergingFrameReader merger = new RunMergingFrameReader(ctx, runCursors, inFrames, sortFields,
                    comparators, recordDesc);
            merger.open();
            try {
                while (merger.nextFrame(outFrame)) {
                    FrameUtils.flushFrame(outFrame, writer);
                }
            } finally {
                merger.close();
            }
            runs.subList(0, inFrames.size()).clear();
            if (!finalPass) {
                runs.add(0, ((RunFileWriter) writer).createReader());
            }
        } finally {
            if (!finalPass) {
                writer.close();
            }
        }
    }
}