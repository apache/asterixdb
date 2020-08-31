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
package org.apache.asterix.runtime.operators.joins.interval.utils.memory;

import java.nio.ByteBuffer;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;

public class RunFileStream {

    private final String key;
    private final IFrame runFileBuffer;
    private final IFrameTupleAppender runFileAppender;
    private RunFileWriter runFileWriter;
    private RunFileReader runFileReader;
    private FileReference runfile;

    private final IHyracksTaskContext ctx;

    private long runFileCounter = 0;
    private long readCount = 0;
    private long writeCount = 0;
    private long totalTupleCount = 0;
    private long previousReadPointer;

    private boolean reading = false;
    private boolean writing = false;

    /**
     * The RunFileSream uses two frames to buffer read and write operations.
     * WorkFlow: CreateRunFileWriter, Write information, close it, flush it,
     * go to the next frame, and repeat.
     *
     * @param ctx
     * @param key
     * @throws HyracksDataException
     */
    public RunFileStream(IHyracksTaskContext ctx, String key) throws HyracksDataException {
        this.ctx = ctx;
        this.key = key;

        // TODO make the stream only use one buffer.
        runFileBuffer = new VSizeFrame(ctx);
        runFileAppender = new FrameTupleAppender(new VSizeFrame(ctx));

    }

    public long getFileCount() {
        return runFileCounter;
    }

    public long getTupleCount() {
        return totalTupleCount;
    }

    public long getReadCount() {
        return readCount;
    }

    public long getWriteCount() {
        return writeCount;
    }

    public void createRunFileWriting() throws HyracksDataException {
        runFileCounter++;
        String prefix = key + '-' + runFileCounter + '-' + this.toString();
        runfile = ctx.getJobletContext().createManagedWorkspaceFile(prefix);
        if (runFileWriter != null) {
            runFileWriter.close();
        }

        runFileWriter = new RunFileWriter(runfile, ctx.getIoManager());
        runFileWriter.open();
        totalTupleCount = 0;
    }

    public void startRunFileWriting() throws HyracksDataException {
        writing = true;
        runFileBuffer.reset();
    }

    public void addToRunFile(IFrameTupleAccessor accessor, int tupleId) throws HyracksDataException {
        if (!runFileAppender.append(accessor, tupleId)) {
            runFileAppender.write(runFileWriter, true);
            writeCount++;
            runFileAppender.append(accessor, tupleId);
        }
        totalTupleCount++;
    }

    public void addToRunFile(FrameTupleCursor cursor) throws HyracksDataException {
        if (!runFileAppender.append(cursor.getAccessor(), cursor.getTupleId())) {
            runFileAppender.write(runFileWriter, true);
            writeCount++;
            runFileAppender.append(cursor.getAccessor(), cursor.getTupleId());
        }
        totalTupleCount++;
    }

    public void startReadingRunFile(FrameTupleCursor cursor) throws HyracksDataException {
        startReadingRunFile(cursor, 0);
    }

    public void startReadingRunFile(FrameTupleCursor cursor, long startOffset) throws HyracksDataException {
        if (runFileReader != null) {
            runFileReader.close();
        }
        reading = true;
        // Create reader
        runFileReader = runFileWriter.createReader();
        runFileReader.open();
        runFileReader.seek(startOffset);
        previousReadPointer = 0;
        // Load first frame
        loadNextBuffer(cursor);
    }

    public boolean loadNextBuffer(FrameTupleCursor cursor) throws HyracksDataException {
        final long tempFrame = runFileReader.position();
        if (runFileReader.nextFrame(runFileBuffer)) {
            previousReadPointer = tempFrame;
            cursor.reset(runFileBuffer.getBuffer());
            readCount++;
            return true;
        }
        return false;
    }

    public void flushRunFile() throws HyracksDataException {
        writing = false;
        // Flush buffer.
        if (runFileAppender.getTupleCount() > 0) {
            runFileAppender.write(runFileWriter, true);
            writeCount++;
        }
        runFileBuffer.reset();
    }

    public void closeRunFileReading() throws HyracksDataException {
        reading = false;
        runFileReader.close();
        previousReadPointer = -1;
    }

    public void close() throws HyracksDataException {
        if (runFileReader != null) {
            runFileReader.close();
        }
        if (runFileWriter != null) {
            runFileWriter.close();
        }
    }

    public void removeRunFile() {
        if (runfile != null) {
            FileUtils.deleteQuietly(runfile.getFile());
        }
    }

    public boolean isReading() {
        return reading;
    }

    public boolean isWriting() {
        return writing;
    }

    public long getReadPointer() {
        if (runFileReader != null) {
            return previousReadPointer;
        }
        return -1;
    }

    public ByteBuffer getAppenderBuffer() {
        return runFileAppender.getBuffer();
    }
}
