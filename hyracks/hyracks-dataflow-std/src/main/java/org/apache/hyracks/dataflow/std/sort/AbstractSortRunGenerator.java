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

import java.util.LinkedList;
import java.util.List;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;

public abstract class AbstractSortRunGenerator implements IRunGenerator {
    protected final List<RunAndMaxFrameSizePair> runAndMaxSizes;

    public AbstractSortRunGenerator() {
        runAndMaxSizes = new LinkedList<>();
    }

    abstract public ISorter getSorter() throws HyracksDataException;

    @Override
    public void open() throws HyracksDataException {
        runAndMaxSizes.clear();
    }

    @Override
    public void close() throws HyracksDataException {
        if (getSorter().hasRemaining()) {
            if (runAndMaxSizes.size() <= 0) {
                getSorter().sort();
            } else {
                flushFramesToRun();
            }
        }
    }

    abstract protected RunFileWriter getRunFileWriter() throws HyracksDataException;

    abstract protected IFrameWriter getFlushableFrameWriter(RunFileWriter writer) throws HyracksDataException;

    protected void flushFramesToRun() throws HyracksDataException {
        getSorter().sort();
        RunFileWriter runWriter = getRunFileWriter();
        IFrameWriter flushWriter = getFlushableFrameWriter(runWriter);
        flushWriter.open();
        int maxFlushedFrameSize;
        try {
            maxFlushedFrameSize = getSorter().flush(flushWriter);
        } finally {
            flushWriter.close();
        }
        runAndMaxSizes.add(new RunAndMaxFrameSizePair(runWriter.createReader(), maxFlushedFrameSize));
        getSorter().reset();
    }

    @Override
    public void fail() throws HyracksDataException {
    }

    @Override
    public List<RunAndMaxFrameSizePair> getRuns() {
        return runAndMaxSizes;
    }
}
