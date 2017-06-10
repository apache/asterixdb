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
package org.apache.hyracks.algebricks.runtime.operators.std;

import java.io.PrintStream;
import java.nio.ByteBuffer;

import org.apache.hyracks.algebricks.data.IAWriter;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputSinkPushRuntime;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class SinkWriterRuntime extends AbstractOneInputSinkPushRuntime {

    private final PrintStream printStream;
    private final IAWriter writer;
    private RecordDescriptor inputRecordDesc;
    private FrameTupleAccessor tAccess;
    private boolean autoClose = false;
    private boolean first = true;

    public SinkWriterRuntime(IAWriter writer, PrintStream printStream, RecordDescriptor inputRecordDesc) {
        this.writer = writer;
        this.printStream = printStream;
        this.inputRecordDesc = inputRecordDesc;
        this.tAccess = new FrameTupleAccessor(inputRecordDesc);
    }

    public SinkWriterRuntime(IAWriter writer, PrintStream printStream, RecordDescriptor inputRecordDesc,
            boolean autoClose) {
        this(writer, printStream, inputRecordDesc);
        this.autoClose = autoClose;
    }

    @Override
    public void open() throws HyracksDataException {
        if (first) {
            first = false;
            tAccess = new FrameTupleAccessor(inputRecordDesc);
            writer.init();
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        tAccess.reset(buffer);
        int nTuple = tAccess.getTupleCount();
        for (int t = 0; t < nTuple; t++) {
            writer.printTuple(tAccess, t);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (autoClose) {
            printStream.close();
        }
    }

    @Override
    public void setInputRecordDescriptor(int index, RecordDescriptor recordDescriptor) {
        this.inputRecordDesc = recordDescriptor;
    }

    @Override
    public void fail() throws HyracksDataException {
        // fail() is a no op. in close we will cleanup
    }

    @Override
    public void flush() throws HyracksDataException {
        // flush() makes no sense to sink operators
    }
}
