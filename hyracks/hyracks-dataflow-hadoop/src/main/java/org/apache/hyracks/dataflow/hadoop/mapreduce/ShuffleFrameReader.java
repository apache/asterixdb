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
package org.apache.hyracks.dataflow.hadoop.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.comm.NoShrinkVSizeFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.collectors.NonDeterministicChannelReader;
import org.apache.hyracks.dataflow.std.sort.ExternalSortRunMerger;
import org.apache.hyracks.dataflow.std.sort.RunAndMaxFrameSizePair;

public class ShuffleFrameReader implements IFrameReader {
    private final IHyracksTaskContext ctx;
    private final NonDeterministicChannelReader channelReader;
    private final HadoopHelper helper;
    private final RecordDescriptor recordDescriptor;
    private final IFrame vframe;
    private List<RunFileWriter> runFileWriters;
    private List<Integer> runFileMaxFrameSize;
    private RunFileReader reader;

    public ShuffleFrameReader(IHyracksTaskContext ctx, NonDeterministicChannelReader channelReader,
            MarshalledWritable<Configuration> mConfig) throws HyracksDataException {
        this.ctx = ctx;
        this.channelReader = channelReader;
        this.helper = new HadoopHelper(mConfig);
        this.recordDescriptor = helper.getMapOutputRecordDescriptor();
        this.vframe = new NoShrinkVSizeFrame(ctx);
    }

    @Override
    public void open() throws HyracksDataException {
        channelReader.open();
        int nSenders = channelReader.getSenderPartitionCount();
        runFileWriters = new ArrayList<RunFileWriter>();
        runFileMaxFrameSize = new ArrayList<>();
        RunInfo[] infos = new RunInfo[nSenders];
        FrameTupleAccessor accessor = new FrameTupleAccessor(recordDescriptor);
        while (true) {
            int entry = channelReader.findNextSender();
            if (entry < 0) {
                break;
            }
            RunInfo info = infos[entry];
            ByteBuffer netBuffer = channelReader.getNextBuffer(entry);
            netBuffer.clear();
            int nBlocks = FrameHelper.deserializeNumOfMinFrame(netBuffer);

            if (nBlocks > 1) {
                netBuffer = getCompleteBuffer(nBlocks, netBuffer, entry);
            }

            accessor.reset(netBuffer, 0, netBuffer.limit());
            int nTuples = accessor.getTupleCount();
            for (int i = 0; i < nTuples; ++i) {
                int tBlockId = IntegerPointable.getInteger(accessor.getBuffer().array(),
                        accessor.getAbsoluteFieldStartOffset(i, HadoopHelper.BLOCKID_FIELD_INDEX));
                if (info == null) {
                    info = new RunInfo();
                    info.reset(tBlockId);
                    infos[entry] = info;
                } else if (info.blockId != tBlockId) {
                    info.close();
                    info.reset(tBlockId);
                }
                info.write(accessor, i);
            }

            if (nBlocks == 1) {
                channelReader.recycleBuffer(entry, netBuffer);
            }
        }
        for (int i = 0; i < infos.length; ++i) {
            RunInfo info = infos[i];
            if (info != null) {
                info.close();
            }
        }

        FileReference outFile = ctx.createManagedWorkspaceFile(ShuffleFrameReader.class.getName() + ".run");
        int framesLimit = helper.getSortFrameLimit(ctx);
        IBinaryComparatorFactory[] comparatorFactories = helper.getSortComparatorFactories();
        IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        List<RunAndMaxFrameSizePair> runs = new LinkedList<>();
        for (int i = 0; i < runFileWriters.size(); i++) {
            runs.add(new RunAndMaxFrameSizePair(runFileWriters.get(i).createDeleteOnCloseReader(), runFileMaxFrameSize
                    .get(i)));
        }
        RunFileWriter rfw = new RunFileWriter(outFile, ctx.getIOManager());
        ExternalSortRunMerger merger = new ExternalSortRunMerger(ctx, null, runs, new int[] { 0 }, comparators, null,
                recordDescriptor, framesLimit, rfw);
        merger.process();

        reader = rfw.createDeleteOnCloseReader();
        reader.open();
    }

    private ByteBuffer getCompleteBuffer(int nBlocks, ByteBuffer netBuffer, int entry) throws HyracksDataException {
        vframe.reset();
        vframe.ensureFrameSize(vframe.getMinSize() * nBlocks);
        FrameUtils.copyWholeFrame(netBuffer, vframe.getBuffer());
        channelReader.recycleBuffer(entry, netBuffer);
        for (int i = 1; i < nBlocks; ++i) {
            netBuffer = channelReader.getNextBuffer(entry);
            netBuffer.clear();
            vframe.getBuffer().put(netBuffer);
            channelReader.recycleBuffer(entry, netBuffer);
        }
        if (vframe.getBuffer().hasRemaining()) { // bigger frame
            FrameHelper.clearRemainingFrame(vframe.getBuffer(), vframe.getBuffer().position());
        }
        vframe.getBuffer().flip();
        return vframe.getBuffer();
    }

    @Override
    public boolean nextFrame(IFrame frame) throws HyracksDataException {
        return reader.nextFrame(frame);
    }

    @Override
    public void close() throws HyracksDataException {
        reader.close();
    }

    private class RunInfo {
        private final IFrame buffer;
        private final FrameTupleAppender fta;

        private FileReference file;
        private RunFileWriter rfw;
        private int blockId;
        private int maxFrameSize = ctx.getInitialFrameSize();

        public RunInfo() throws HyracksDataException {
            buffer = new VSizeFrame(ctx);
            fta = new FrameTupleAppender();
        }

        public void reset(int blockId) throws HyracksDataException {
            this.blockId = blockId;
            this.maxFrameSize = ctx.getInitialFrameSize();
            fta.reset(buffer, true);
            try {
                file = ctx.createManagedWorkspaceFile(ShuffleFrameReader.class.getName() + ".run");
                rfw = new RunFileWriter(file, ctx.getIOManager());
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        public void write(FrameTupleAccessor accessor, int tIdx) throws HyracksDataException {
            if (!fta.append(accessor, tIdx)) {
                flush();
                if (!fta.append(accessor, tIdx)) {
                    throw new HyracksDataException("Record size ("
                            + (accessor.getTupleEndOffset(tIdx) - accessor.getTupleStartOffset(tIdx))
                            + ") larger than frame size (" + fta.getBuffer().capacity() + ")");
                }
            }
        }

        public void close() throws HyracksDataException {
            flush();
            rfw.close();
            runFileWriters.add(rfw);
            runFileMaxFrameSize.add(maxFrameSize);
        }

        private void flush() throws HyracksDataException {
            if (fta.getTupleCount() <= 0) {
                return;
            }
            maxFrameSize = buffer.getFrameSize() > maxFrameSize ? buffer.getFrameSize() : maxFrameSize;
            rfw.nextFrame((ByteBuffer) buffer.getBuffer().clear());
            fta.reset(buffer, true);
        }
    }
}
