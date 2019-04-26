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
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;

public abstract class AbstractSortRunGenerator implements IRunGenerator {

    private final List<GeneratedRunFileReader> generatedRunFileReaders;

    public AbstractSortRunGenerator() {
        generatedRunFileReaders = new LinkedList<>();
    }

    /**
     * Null could be returned. Caller should check if it not null.
     * @return the sorter associated with the run generator or null if there is no sorter.
     */
    abstract public ISorter getSorter();

    @Override
    public void open() throws HyracksDataException {
        generatedRunFileReaders.clear();
    }

    @Override
    public void close() throws HyracksDataException {
        ISorter sorter = getSorter();
        if (sorter != null && sorter.hasRemaining()) {
            if (generatedRunFileReaders.size() <= 0) {
                sorter.sort();
            } else {
                flushFramesToRun();
            }
        }
    }

    abstract protected RunFileWriter getRunFileWriter() throws HyracksDataException;

    abstract protected IFrameWriter getFlushableFrameWriter(RunFileWriter writer) throws HyracksDataException;

    // assumption is that there will always be a sorter (i.e. sorter is not null)
    void flushFramesToRun() throws HyracksDataException {
        ISorter sorter = getSorter();
        sorter.sort();
        RunFileWriter runWriter = getRunFileWriter();
        IFrameWriter flushWriter = getFlushableFrameWriter(runWriter);
        flushWriter.open();
        try {
            sorter.flush(flushWriter);
        } catch (Exception e) {
            flushWriter.fail();
            throw e;
        } finally {
            flushWriter.close();
        }
        generatedRunFileReaders.add(runWriter.createDeleteOnCloseReader());
        sorter.reset();
    }

    @Override
    public void fail() throws HyracksDataException {
    }

    @Override
    public List<GeneratedRunFileReader> getRuns() {
        return generatedRunFileReaders;
    }
}
