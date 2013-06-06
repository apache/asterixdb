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
package edu.uci.ics.hyracks.algebricks.runtime.operators.std;

import java.io.PrintStream;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IAWriter;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputSinkPushRuntime;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class SinkWriterRuntime extends AbstractOneInputSinkPushRuntime {

    private final IHyracksTaskContext ctx;
    private final PrintStream printStream;
    private final IAWriter writer;
    private RecordDescriptor inputRecordDesc;
    private FrameTupleAccessor tAccess;
    private boolean autoClose = false;
    private boolean first = true;

    public SinkWriterRuntime(IAWriter writer, IHyracksTaskContext ctx, PrintStream printStream,
            RecordDescriptor inputRecordDesc) {
        this.writer = writer;
        this.ctx = ctx;
        this.printStream = printStream;
        this.inputRecordDesc = inputRecordDesc;
        this.tAccess = new FrameTupleAccessor(ctx.getFrameSize(), inputRecordDesc);
    }

    public SinkWriterRuntime(IAWriter writer, IHyracksTaskContext ctx, PrintStream printStream,
            RecordDescriptor inputRecordDesc, boolean autoClose) {
        this(writer, ctx, printStream, inputRecordDesc);
        this.autoClose = autoClose;
    }

    @Override
    public void open() throws HyracksDataException {
        if (first) {
            first = false;
            tAccess = new FrameTupleAccessor(ctx.getFrameSize(), inputRecordDesc);
            try {
                writer.init();
            } catch (AlgebricksException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        tAccess.reset(buffer);
        int nTuple = tAccess.getTupleCount();
        for (int t = 0; t < nTuple; t++) {
            try {
                writer.printTuple(tAccess, t);
            } catch (AlgebricksException ae) {
                throw new HyracksDataException(ae);
            }
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
    }
}
