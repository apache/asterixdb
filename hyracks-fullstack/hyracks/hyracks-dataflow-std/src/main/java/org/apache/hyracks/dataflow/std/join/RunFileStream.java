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
package org.apache.hyracks.dataflow.std.join;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;

public class RunFileStream {

    private static final Logger LOGGER = Logger.getLogger(RunFileStream.class.getName());

    private final String key;
    private final IFrame runFileBuffer;
    private final IFrameTupleAppender runFileAppender;
    private RunFileWriter runFileWriter;
    private RunFileReader runFileReader;
    private final IRunFileStreamStatus status;

    private final IHyracksTaskContext ctx;

    private long runFileCounter = 0;
    private long readCount = 0;
    private long writeCount = 0;

    public RunFileStream(IHyracksTaskContext ctx, String key, IRunFileStreamStatus status) throws HyracksDataException {
        this.ctx = ctx;
        this.key = key;
        this.status = status;

        runFileBuffer = new VSizeFrame(ctx);
        runFileAppender = new FrameTupleAppender(new VSizeFrame(ctx));
    }

    public long getReadCount() {
        return readCount;
    }

    public long getWriteCount() {
        return writeCount;
    }

    public void startRunFile() throws HyracksDataException {
        readCount = 0;
        writeCount = 0;
        runFileCounter++;

        status.setRunFileWriting(true);
        String prefix = this.getClass().getSimpleName() + '-' + key + '-' + Long.toString(runFileCounter) + '-'
                + this.toString();
        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(prefix);
        runFileWriter = new RunFileWriter(file, ctx.getIOManager());
        runFileWriter.open();
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("A new run file has been started (key: " + key + ", number: " + runFileCounter + ", file: "
                    + file + ").");
        }
    }

    public void addToRunFile(ITupleAccessor accessor) throws HyracksDataException {
        int idx = accessor.getTupleId();
        if (!runFileAppender.append(accessor, idx)) {
            runFileAppender.write(runFileWriter, true);
            writeCount++;
            runFileAppender.append(accessor, idx);
        }
    }

    public void openRunFile(ITupleAccessor accessor) throws HyracksDataException {
        status.setRunFileReading(true);

        // Create reader
        runFileReader = runFileWriter.createDeleteOnCloseReader();
        runFileReader.open();

        // Load first frame
        loadNextBuffer(accessor);
    }

    public void resetReader(ITupleAccessor accessor) throws HyracksDataException {
        if (status.isRunFileWriting()) {
            flushAndStopRunFile(accessor);
            openRunFile(accessor);
        } else {
            runFileReader.reset();

            // Load first frame
            loadNextBuffer(accessor);
        }
    }

    public boolean loadNextBuffer(ITupleAccessor accessor) throws HyracksDataException {
        if (runFileReader.nextFrame(runFileBuffer)) {
            accessor.reset(runFileBuffer.getBuffer());
            accessor.next();
            readCount++;
            return true;
        }
        return false;
    }

    public void flushAndStopRunFile(ITupleAccessor accessor) throws HyracksDataException {
        status.setRunFileWriting(false);

        // Copy left over tuples into new run file.
        if (status.isRunFileReading()) {
            if (!accessor.exists()) {
                loadNextBuffer(accessor);
            }
            while (accessor.exists()) {
                addToRunFile(accessor);
                accessor.next();
                if (!accessor.exists()) {
                    loadNextBuffer(accessor);
                }
            }
        }

        // Flush buffer.
        if (runFileAppender.getTupleCount() > 0) {
            runFileAppender.write(runFileWriter, true);
            writeCount++;
        }
    }

    public void closeRunFile() throws HyracksDataException {
        status.setRunFileReading(false);
        runFileReader.close();
    }

    public void close() throws HyracksDataException {
        if (runFileReader != null) {
            runFileReader.close();
        }
        if (runFileWriter != null) {
            runFileWriter.close();
        }
    }

}
