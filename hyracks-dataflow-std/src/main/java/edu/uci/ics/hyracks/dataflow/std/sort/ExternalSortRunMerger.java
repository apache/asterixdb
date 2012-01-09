package edu.uci.ics.hyracks.dataflow.std.sort;

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
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;

/**
 * @author pouria
 *         This class defines the logic for merging the run, generated during
 *         the first phase of external sort (for both sorting without replacement
 *         selection and with it). For the case with replacement selection, this
 *         code also takes the limit on the output into account (if specified).
 *         If number of input runs is less than the available memory frames,
 *         then merging can be done in one pass, by allocating one buffer per
 *         run, and one buffer as the output buffer. A priorityQueue is used to
 *         find the top tuple at each iteration, among all the runs' heads in
 *         memory (check RunMergingFrameReader for more details). Otherwise,
 *         assuming that we have R runs and M memory buffers, where (R > M), we
 *         first merge first (M-1) runs and create a new sorted run, out of
 *         them. Discarding the first (M-1) runs, now merging procedure gets
 *         applied recursively on the (R-M+2) remaining runs using the M memory
 *         buffers.
 *         For the case of replacement selection, if outputLimit is specified,
 *         once the final pass is done on the runs (which is the pass
 *         that generates the final sorted output), as soon as the output size
 *         hits the output limit, the process stops, closes, and returns.
 */

public class ExternalSortRunMerger {

    private final IHyracksTaskContext ctx;
    private final List<IFrameReader> runs;
    private final int[] sortFields;
    private final IBinaryComparator[] comparators;
    private final RecordDescriptor recordDesc;
    private final int framesLimit;
    private final IFrameWriter writer;
    private List<ByteBuffer> inFrames;
    private ByteBuffer outFrame;
    private FrameTupleAppender outFrameAppender;

    private FrameSorter frameSorter; //Used in External sort, no replacement selection
    private FrameTupleAccessor outFrameAccessor; //Used in External sort, with replacement selection
    private final int outputLimit; //Used in External sort, with replacement selection and limit on output size
    private int currentSize; //Used in External sort, with replacement selection and limit on output size

    //Constructor for external sort, no replacement selection
    public ExternalSortRunMerger(IHyracksTaskContext ctx, FrameSorter frameSorter, List<IFrameReader> runs,
            int[] sortFields, IBinaryComparator[] comparators, RecordDescriptor recordDesc, int framesLimit,
            IFrameWriter writer) {
        this.ctx = ctx;
        this.frameSorter = frameSorter;
        this.runs = new LinkedList<IFrameReader>(runs);
        this.sortFields = sortFields;
        this.comparators = comparators;
        this.recordDesc = recordDesc;
        this.framesLimit = framesLimit;
        this.writer = writer;
        this.outputLimit = -1;
    }

    //Constructor for external sort with replacement selection
    public ExternalSortRunMerger(IHyracksTaskContext ctx, int outputLimit, List<IFrameReader> runs, int[] sortFields,
            IBinaryComparator[] comparators, RecordDescriptor recordDesc, int framesLimit, IFrameWriter writer) {
        this.ctx = ctx;
        this.runs = new LinkedList<IFrameReader>(runs);
        this.sortFields = sortFields;
        this.comparators = comparators;
        this.recordDesc = recordDesc;
        this.framesLimit = framesLimit;
        this.writer = writer;
        this.outputLimit = outputLimit;
        this.currentSize = 0;
        this.frameSorter = null;
    }

    public void process() throws HyracksDataException {
        writer.open();
        try {
            if (runs.size() <= 0) {
                if (frameSorter != null && frameSorter.getFrameCount() > 0) {
                    frameSorter.flushFrames(writer);
                }
                /** recycle sort buffer */
                frameSorter = null;
                System.gc();

            } else {
            	/** recycle sort buffer */
                frameSorter = null;
                System.gc();

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
        } catch (Exception e) {
            writer.fail();
            throw new HyracksDataException(e);
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

    public void processWithReplacementSelection() throws HyracksDataException {
        writer.open();
        try {
            outFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), recordDesc);
            outFrame = ctx.allocateFrame();
            outFrameAppender = new FrameTupleAppender(ctx.getFrameSize());
            outFrameAppender.reset(outFrame, true);
            if (runs.size() == 1) {
                if (outputLimit < 1) {
                    runs.get(0).open();
                    ByteBuffer nextFrame = ctx.allocateFrame();
                    while (runs.get(0).nextFrame(nextFrame)) {
                        FrameUtils.flushFrame(nextFrame, writer);
                        outFrameAppender.reset(nextFrame, true);
                    }
                    System.gc();
                    return;
                }
                //Limit on the output size
                int totalCount = 0;
                runs.get(0).open();
                FrameTupleAccessor fta = new FrameTupleAccessor(ctx.getFrameSize(), recordDesc);
                ByteBuffer nextFrame = ctx.allocateFrame();
                while (totalCount <= outputLimit && runs.get(0).nextFrame(nextFrame)) {
                    fta.reset(nextFrame);
                    int tupCount = fta.getTupleCount();
                    if ((totalCount + tupCount) < outputLimit) {
                        FrameUtils.flushFrame(nextFrame, writer);
                        totalCount += tupCount;
                        continue;
                    }
                    // The very last buffer, which exceeds the limit
                    int copyCount = outputLimit - totalCount;
                    outFrameAppender.reset(outFrame, true);
                    for (int i = 0; i < copyCount; i++) {
                        if (!outFrameAppender.append(fta, i)) {
                            throw new IllegalStateException();
                        }
                        totalCount++;
                    }
                }
                if (outFrameAppender.getTupleCount() > 0) {
                    FrameUtils.flushFrame(outFrame, writer);
                    outFrameAppender.reset(outFrame, true);
                }
                System.gc();
                return;
            }
            // More than one run, actual merging is needed
            System.gc();
            inFrames = new ArrayList<ByteBuffer>();
            for (int i = 0; i < framesLimit - 1; ++i) {
                inFrames.add(ctx.allocateFrame());
            }
            while (runs.size() > 0) {
                try {
                    doPassWithReplacementSelection(runs);
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }

        } catch (Exception e) {
            writer.fail();
            throw new HyracksDataException(e);
        } finally {
            writer.close();
        }
    }

    // creates a new run from runs that can fit in memory.
    private void doPassWithReplacementSelection(List<IFrameReader> runs) throws HyracksDataException {
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
                    if (outputLimit > 0 && finalPass) {
                        outFrameAccessor.reset(outFrame);
                        int count = outFrameAccessor.getTupleCount();
                        if ((currentSize + count) > outputLimit) {
                            ByteBuffer b = ctx.allocateFrame();
                            FrameTupleAppender partialAppender = new FrameTupleAppender(ctx.getFrameSize());
                            partialAppender.reset(b, true);
                            int copyCount = outputLimit - currentSize;
                            for (int i = 0; i < copyCount; i++) {
                                partialAppender.append(outFrameAccessor, i);
                                currentSize++;
                            }
                            FrameUtils.makeReadable(b);
                            FrameUtils.flushFrame(b, writer);
                            break;
                        } else {
                            FrameUtils.flushFrame(outFrame, writer);
                            currentSize += count;
                        }
                    } else {
                        FrameUtils.flushFrame(outFrame, writer);
                    }
                }
            } finally {
                merger.close();
            }

            if (outputLimit > 0 && finalPass && (currentSize >= outputLimit)) {
                runs.clear();
                return;
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
