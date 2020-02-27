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

package org.apache.hyracks.dataflow.common.io;

import java.io.DataOutput;
import java.util.Random;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for RunFileReader and RunFileWriter.
 * <p>
 * Runs test for single and multiple frames written to the run file.
 * The reading can be done in sequence, out of sequence, reverse
 * and in separate sessions between writing.
 *
 * @see org.apache.hyracks.dataflow.common.io.RunFileReader
 * @see org.apache.hyracks.dataflow.common.io.RunFileWriter
 */

public class RunFileTest {

    private static final int TEST_FRAME_SIZE = 256;
    private static final int TEST_FRAME_SIZE_ALTERNATE = 512;
    private static final int MULTIPLE_FRAMES = 5;
    private static final int NUMBER_OF_TUPLES = 10;
    private static final int NUMBER_OF_TUPLES_ALTERNATE = 100;
    private static final int FIELD_COUNT = 2;

    private final IHyracksTaskContext ctx = TestUtils.create(TEST_FRAME_SIZE);

    private RunFileWriter writer;

    @Before
    public void setup() throws HyracksDataException {
        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile("RunFileTest");
        IIOManager ioManager = ctx.getIoManager();
        writer = new RunFileWriter(file, ioManager);
    }

    @Test
    public void testSingleFrame() throws HyracksDataException {

        //Declare fields
        IFrame frame = new VSizeFrame(ctx);
        IFrame readFrame = new VSizeFrame(ctx);

        // Writer test
        writer.open();
        writeFrame(frame);

        // Reading what was written
        RunFileReader reader;
        reader = writer.createDeleteOnCloseReader();
        writer.close();
        reader.open();
        reader.nextFrame(readFrame);
        Assert.assertArrayEquals("Reading frame bytes", frame.getBuffer().array(), readFrame.getBuffer().array());
        reader.close();
    }

    @Test
    public void testMultipleFrame() throws HyracksDataException {

        //Declare fields
        IFrame[] frames = getFrames(MULTIPLE_FRAMES);

        // Writer test
        writer.open();
        writeFrames(frames);
        IFrame[] readFrames = getFrames(MULTIPLE_FRAMES);

        // Reading what was written
        RunFileReader reader;
        reader = writer.createDeleteOnCloseReader();
        writer.close();
        reader.open();
        readFrames(frames, readFrames, reader);
        reader.close();
    }

    @Test
    public void testMultipleFrameMultipleReads() throws HyracksDataException {

        //Declare Fields
        IFrame[] frames = getFrames(MULTIPLE_FRAMES);

        // Writer test
        writer.open();
        writeFrames(frames);
        IFrame[] readFrames = getFrames(MULTIPLE_FRAMES);

        // Reading what was written
        RunFileReader reader = writer.createDeleteOnCloseReader();
        writer.close();
        reader.open();
        readFrames(frames, readFrames, reader);
        reader.seek(0);
        readFrames(frames, readFrames, reader);
        reader.close();
    }

    @Test
    public void testMultipleFrameRandomReads() throws HyracksDataException {

        //Declare fields
        IFrame[] frames = getFrames(MULTIPLE_FRAMES);
        IFrame[] readFrames = getFrames(MULTIPLE_FRAMES);
        long[] frameOffsets = new long[frames.length];

        // Writer test
        writer.open();
        writeFramesWithOffsets(frames, frameOffsets, 1);

        // Reading what was written
        RunFileReader reader;
        reader = writer.createDeleteOnCloseReader();
        writer.close();
        reader.open();
        readFrames(frames, readFrames, reader);
        readFramesBackwards(frames, readFrames, frameOffsets, reader);
        reader.close();
    }

    @Test
    public void testMultipleDifferentSizedFrames() throws HyracksDataException {

        //Declare fields
        IFrame[] frames = getFramesAlternate(MULTIPLE_FRAMES);
        IFrame[] readFrames = getFrames(MULTIPLE_FRAMES);
        long[] frameOffsets = new long[frames.length];

        // Writer test
        writer.open();
        writeFramesWithDifferentSizes(frames, frameOffsets);

        // Reading what was written
        RunFileReader reader;
        reader = writer.createDeleteOnCloseReader();
        writer.close();
        reader.open();
        readFrames(frames, readFrames, reader);
        readFramesBackwards(frames, readFrames, frameOffsets, reader);
        reader.close();
    }

    @Test
    public void testMultipleWriteReadSessions() throws HyracksDataException {

        //Declare fields
        IFrame[] framesAll = getFrames(MULTIPLE_FRAMES * 2);
        IFrame[] readFrames = getFrames(framesAll.length);
        long[] frameOffsets = new long[framesAll.length];

        // Writer test
        writer.open();
        writeFramesWithOffsets(framesAll, frameOffsets, 1);

        // Reading what was written
        readMultipleFramesForwardsAndBackwards(framesAll, readFrames, frameOffsets, 1);

        // Second write session
        writeFramesWithOffsets(framesAll, frameOffsets, 2);

        // Reading what was written in both sections
        readMultipleFramesForwardsAndBackwards(framesAll, readFrames, frameOffsets, 2);
        writer.close();
    }

    private IFrame[] getFrames(int count) throws HyracksDataException {
        IFrame[] frames = new IFrame[count];
        for (int f = 0; f < frames.length; f++) {
            frames[f] = new VSizeFrame(ctx);
        }
        return frames;
    }

    private IFrame[] getFramesAlternate(int count) throws HyracksDataException {
        if (count < 2) {
            throw new HyracksDataException("Must provide more than one frame.");
        }
        IFrame[] frames = new IFrame[count];
        frames[0] = new VSizeFrame(ctx);
        frames[1] = new VSizeFrame(ctx, TEST_FRAME_SIZE_ALTERNATE);
        for (int f = 2; f < frames.length; f++) {
            frames[f] = new VSizeFrame(ctx);
        }
        return frames;
    }

    public void writeTupleToFrame(FrameTupleAppender appender, int tupleCount) throws HyracksDataException {

        ArrayTupleBuilder tb = new ArrayTupleBuilder(FIELD_COUNT);
        DataOutput dos = tb.getDataOutput();

        Random rnd = new Random();
        rnd.setSeed(50);
        for (int i = 0; i < tupleCount; i++) {
            int f0 = rnd.nextInt() % 100000;
            int f1 = 5;

            tb.reset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
            tb.addFieldEndOffset();

            if (appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                break;
            }
        }
    }

    public void writeFrame(IFrame frame) throws HyracksDataException {

        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(frame, true);
        writeTupleToFrame(appender, NUMBER_OF_TUPLES);

        writer.nextFrame(frame.getBuffer());
    }

    public void writeFrames(IFrame[] frames) throws HyracksDataException {

        FrameTupleAppender appender = new FrameTupleAppender();

        for (int f = 0; f < MULTIPLE_FRAMES; f++) {
            appender.reset(frames[f], true);
            writeTupleToFrame(appender, NUMBER_OF_TUPLES);
            writer.nextFrame(frames[f].getBuffer());
        }
    }

    public void writeFramesWithOffsets(IFrame[] frames, long[] frameOffsets, int writeSession)
            throws HyracksDataException {

        FrameTupleAppender appender = new FrameTupleAppender();

        for (int f = 0; f < MULTIPLE_FRAMES * writeSession; f++) {
            appender.reset(frames[f], true);
            writeTupleToFrame(appender, NUMBER_OF_TUPLES);
            frameOffsets[f] = writer.getFileSize();
            writer.nextFrame(frames[f].getBuffer());
        }
    }

    public void writeFramesWithDifferentSizes(IFrame[] frames, long[] frameOffsets) throws HyracksDataException {

        FrameTupleAppender appender = new FrameTupleAppender();

        for (int f = 0; f < frames.length; f++) {
            appender.reset(frames[f], false);
            int tupleCount = (f == 1 ? NUMBER_OF_TUPLES_ALTERNATE : NUMBER_OF_TUPLES);
            writeTupleToFrame(appender, tupleCount);
            frameOffsets[f] = writer.getFileSize();
            writer.nextFrame(frames[f].getBuffer());
        }
    }

    public void readFrames(IFrame[] frames, IFrame[] readFrames, RunFileReader reader) throws HyracksDataException {

        for (int f = 0; f < frames.length; f++) {
            reader.nextFrame(readFrames[f]);
            Assert.assertArrayEquals("Reading frame[" + f + "] bytes", frames[f].getBuffer().array(),
                    readFrames[f].getBuffer().array());
        }
    }

    public void readFramesBackwards(IFrame[] frames, IFrame[] readFrames, long[] frameOffsets, RunFileReader reader)
            throws HyracksDataException {

        for (int f = frames.length - 1; f >= 0; f--) {
            reader.seek(frameOffsets[f]);
            reader.nextFrame(readFrames[f]);
            Assert.assertArrayEquals("Reading backwards frame[" + f + "] bytes", frames[f].getBuffer().array(),
                    readFrames[f].getBuffer().array());
        }
    }

    public void readMultipleFramesForwardsAndBackwards(IFrame[] framesAll, IFrame[] readFrames, long[] frameOffsets,
            int readSession) throws HyracksDataException {

        RunFileReader reader;
        reader = writer.createReader();
        reader.open();
        for (int f = 0; f < MULTIPLE_FRAMES * readSession; f++) {
            reader.nextFrame(readFrames[f]);
            Assert.assertArrayEquals("Reading frame[" + f + "] bytes", framesAll[f].getBuffer().array(),
                    readFrames[f].getBuffer().array());
        }
        for (int f = MULTIPLE_FRAMES * readSession - 1; f >= 0; f--) {
            reader.seek(frameOffsets[f]);
            reader.nextFrame(readFrames[f]);
            Assert.assertArrayEquals("Reading backwards frame[" + f + "] bytes", framesAll[f].getBuffer().array(),
                    readFrames[f].getBuffer().array());
        }
        reader.close();
    }
}
