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
package edu.uci.ics.hyracks.dataflow.hadoop.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.hyracks.api.channels.IInputChannel;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.collectors.NonDeterministicChannelReader;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortRunMerger;

public class ShuffleFrameReader implements IFrameReader {
    private final IHyracksTaskContext ctx;
    private final NonDeterministicChannelReader channelReader;
    private final HadoopHelper helper;
    private final RecordDescriptor recordDescriptor;
    private List<RunFileWriter> runFileWriters;
    private RunFileReader reader;

    public ShuffleFrameReader(IHyracksTaskContext ctx, NonDeterministicChannelReader channelReader,
            MarshalledWritable<Configuration> mConfig) throws HyracksDataException {
        this.ctx = ctx;
        this.channelReader = channelReader;
        helper = new HadoopHelper(mConfig);
        this.recordDescriptor = helper.getMapOutputRecordDescriptor();
    }

    @Override
    public void open() throws HyracksDataException {
        channelReader.open();
        int nSenders = channelReader.getSenderPartitionCount();
        runFileWriters = new ArrayList<RunFileWriter>();
        RunInfo[] infos = new RunInfo[nSenders];
        FrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        IInputChannel[] channels = channelReader.getChannels();
        while (true) {
            int entry = channelReader.findNextSender();
            if (entry < 0) {
                break;
            }
            RunInfo info = infos[entry];
            IInputChannel channel = channels[entry];
            ByteBuffer netBuffer = channel.getNextBuffer();
            accessor.reset(netBuffer);
            int nTuples = accessor.getTupleCount();
            for (int i = 0; i < nTuples; ++i) {
                int tBlockId = IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(),
                        FrameUtils.getAbsoluteFieldStartOffset(accessor, i, HadoopHelper.BLOCKID_FIELD_INDEX));
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
            channel.recycleBuffer(netBuffer);
        }
        for (int i = 0; i < infos.length; ++i) {
            RunInfo info = infos[i];
            if (info != null) {
                info.close();
            }
        }
        infos = null;

        FileReference outFile = ctx.createManagedWorkspaceFile(ShuffleFrameReader.class.getName() + ".run");
        int framesLimit = helper.getSortFrameLimit(ctx);
        IBinaryComparatorFactory[] comparatorFactories = helper.getSortComparatorFactories();
        IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        List<IFrameReader> runs = new LinkedList<IFrameReader>();
        for (RunFileWriter rfw : runFileWriters) {
            runs.add(rfw.createReader());
        }
        RunFileWriter rfw = new RunFileWriter(outFile, ctx.getIOManager());
        ExternalSortRunMerger merger = new ExternalSortRunMerger(ctx, null, runs, new int[] { 0 }, comparators, null,
                recordDescriptor, framesLimit, rfw);
        merger.process();

        reader = rfw.createReader();
        reader.open();
    }

    @Override
    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
        return reader.nextFrame(buffer);
    }

    @Override
    public void close() throws HyracksDataException {
        reader.close();
    }

    private class RunInfo {
        private final ByteBuffer buffer;
        private final FrameTupleAppender fta;

        private FileReference file;
        private RunFileWriter rfw;
        private int blockId;

        public RunInfo() throws HyracksDataException {
            buffer = ctx.allocateFrame();
            fta = new FrameTupleAppender(ctx.getFrameSize());
        }

        public void reset(int blockId) throws HyracksDataException {
            this.blockId = blockId;
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
        }

        private void flush() throws HyracksDataException {
            if (fta.getTupleCount() <= 0) {
                return;
            }
            buffer.limit(buffer.capacity());
            buffer.position(0);
            rfw.nextFrame(buffer);
            fta.reset(buffer, true);
        }
    }
}
