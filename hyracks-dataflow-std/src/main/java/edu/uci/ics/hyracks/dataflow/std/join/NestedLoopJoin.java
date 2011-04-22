package edu.uci.ics.hyracks.dataflow.std.join;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparator;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;

public class NestedLoopJoin {
    private final FrameTupleAccessor accessorInner;
    private final FrameTupleAccessor accessorOuter;
    private final FrameTupleAppender appender;
    private final ITuplePairComparator tpComparator;
    private final ByteBuffer outBuffer;
    private final ByteBuffer innerBuffer;
    private final List<ByteBuffer> outBuffers;
    private final int memSize;
    private final IHyracksStageletContext ctx;
    private RunFileReader runFileReader;
    private int currentMemSize = 0;
    private final RunFileWriter runFileWriter;

    public NestedLoopJoin(IHyracksStageletContext ctx, FrameTupleAccessor accessor0, FrameTupleAccessor accessor1,
            ITuplePairComparator comparators, int memSize) throws HyracksDataException {
        this.accessorInner = accessor1;
        this.accessorOuter = accessor0;
        this.appender = new FrameTupleAppender(ctx.getFrameSize());
        this.tpComparator = comparators;
        this.outBuffer = ctx.allocateFrame();
        this.innerBuffer = ctx.allocateFrame();
        this.appender.reset(outBuffer, true);
        this.outBuffers = new ArrayList<ByteBuffer>();
        this.memSize = memSize;
        this.ctx = ctx;

        FileReference file = ctx.getJobletContext().createWorkspaceFile(
                this.getClass().getSimpleName() + this.toString());
        runFileWriter = new RunFileWriter(file, ctx.getIOManager());
        runFileWriter.open();
    }

    public void cache(ByteBuffer buffer) throws HyracksDataException {
        runFileWriter.nextFrame(buffer);
        System.out.println(runFileWriter.getFileSize());
    }

    public void join(ByteBuffer outerBuffer, IFrameWriter writer) throws HyracksDataException {
        if (outBuffers.size() < memSize - 3) {
            ByteBuffer outerBufferCopy = ctx.allocateFrame();
            FrameUtils.copy(outerBuffer, outerBufferCopy);
            outBuffers.add(outerBufferCopy);
            currentMemSize++;
            return;
        }
        if (currentMemSize < memSize - 3) {
            FrameUtils.copy(outerBuffer, outBuffers.get(currentMemSize));
            currentMemSize++;
            return;
        }
        for (ByteBuffer outBuffer : outBuffers) {
            runFileReader = runFileWriter.createReader();
            runFileReader.open();
            while (runFileReader.nextFrame(innerBuffer)) {
                blockJoin(outBuffer, innerBuffer, writer);
            }
            runFileReader.close();
        }
        currentMemSize = 0;
    }

    private void blockJoin(ByteBuffer outerBuffer, ByteBuffer innerBuffer, IFrameWriter writer)
            throws HyracksDataException {
        accessorOuter.reset(outerBuffer);
        accessorInner.reset(innerBuffer);
        int tupleCount0 = accessorOuter.getTupleCount();
        int tupleCount1 = accessorInner.getTupleCount();

        for (int i = 0; i < tupleCount0; ++i) {
            for (int j = 0; j < tupleCount1; ++j) {
                int c = compare(accessorOuter, i, accessorInner, j);
                if (c == 0) {
                    if (!appender.appendConcat(accessorOuter, i, accessorInner, j)) {
                        flushFrame(outBuffer, writer);
                        appender.reset(outBuffer, true);
                        if (!appender.appendConcat(accessorOuter, i, accessorInner, j)) {
                            throw new IllegalStateException();
                        }
                    }
                }
            }
        }
    }

    public void closeCache() throws HyracksDataException {
        if (runFileWriter != null) {
            runFileWriter.close();
        }
    }

    public void closeJoin(IFrameWriter writer) throws HyracksDataException {
        for (ByteBuffer outBuffer : outBuffers) {
            runFileReader = runFileWriter.createReader();
            runFileReader.open();
            while (runFileReader.nextFrame(innerBuffer)) {
                blockJoin(outBuffer, innerBuffer, writer);
            }
            runFileReader.close();
        }
        outBuffers.clear();
        currentMemSize = 0;

        if (appender.getTupleCount() > 0) {
            flushFrame(outBuffer, writer);
        }
    }

    private void flushFrame(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        buffer.position(0);
        buffer.limit(buffer.capacity());
        writer.nextFrame(buffer);
        buffer.position(0);
        buffer.limit(buffer.capacity());
    }

    private int compare(FrameTupleAccessor accessor0, int tIndex0, FrameTupleAccessor accessor1, int tIndex1)
            throws HyracksDataException {
        int c = tpComparator.compare(accessor0, tIndex0, accessor1, tIndex1);
        if (c != 0) {
            return c;
        }
        return 0;
    }
}
