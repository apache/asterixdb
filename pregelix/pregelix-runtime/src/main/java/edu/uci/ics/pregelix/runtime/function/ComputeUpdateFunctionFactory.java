/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.pregelix.runtime.function;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.ArrayListWritable;
import edu.uci.ics.pregelix.api.util.ArrayListWritable.ArrayIterator;
import edu.uci.ics.pregelix.api.util.FrameTupleUtils;
import edu.uci.ics.pregelix.dataflow.std.base.IUpdateFunction;
import edu.uci.ics.pregelix.dataflow.std.base.IUpdateFunctionFactory;
import edu.uci.ics.pregelix.dataflow.util.ResetableByteArrayOutputStream;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ComputeUpdateFunctionFactory implements IUpdateFunctionFactory {
    private static final long serialVersionUID = 1L;
    public static IUpdateFunctionFactory INSTANCE = new ComputeUpdateFunctionFactory();

    @Override
    public IUpdateFunction createFunction() {
        return new IUpdateFunction() {
            // for writing intermediate data
            private final ArrayTupleBuilder tbMsg = new ArrayTupleBuilder(2);
            private final ArrayTupleBuilder tbAlive = new ArrayTupleBuilder(2);
            private final ArrayTupleBuilder tbTerminate = new ArrayTupleBuilder(1);

            // for writing out to message channel
            private IFrameWriter writerMsg;
            private FrameTupleAppender appenderMsg;
            private ByteBuffer bufferMsg;

            // for writing out to alive message channel
            private IFrameWriter writerAlive;
            private FrameTupleAppender appenderAlive;
            private ByteBuffer bufferAlive;
            private boolean pushAlive;

            // for writing out termination detection control channel
            private IFrameWriter writerTerminate;
            private FrameTupleAppender appenderTerminate;
            private ByteBuffer bufferTerminate;
            private boolean terminate = true;

            private Vertex vertex;
            private ResetableByteArrayOutputStream bbos = new ResetableByteArrayOutputStream();
            private DataOutput output = new DataOutputStream(bbos);

            private ArrayIterator msgIterator = new ArrayIterator();
            private final List<IFrameWriter> writers = new ArrayList<IFrameWriter>();
            private final List<FrameTupleAppender> appenders = new ArrayList<FrameTupleAppender>();
            private final List<ArrayTupleBuilder> tbs = new ArrayList<ArrayTupleBuilder>();

            @Override
            public void open(IHyracksTaskContext ctx, RecordDescriptor rd, IFrameWriter... writers)
                    throws HyracksDataException {
                this.writerMsg = writers[0];
                this.bufferMsg = ctx.allocateFrame();
                this.appenderMsg = new FrameTupleAppender(ctx.getFrameSize());
                this.appenderMsg.reset(bufferMsg, true);
                this.writers.add(writerMsg);
                this.appenders.add(appenderMsg);

                this.writerTerminate = writers[1];
                this.bufferTerminate = ctx.allocateFrame();
                this.appenderTerminate = new FrameTupleAppender(ctx.getFrameSize());
                this.appenderTerminate.reset(bufferTerminate, true);

                if (writers.length > 2) {
                    this.writerAlive = writers[2];
                    this.bufferAlive = ctx.allocateFrame();
                    this.appenderAlive = new FrameTupleAppender(ctx.getFrameSize());
                    this.appenderAlive.reset(bufferAlive, true);
                    this.pushAlive = true;
                    this.writers.add(writerAlive);
                    this.appenders.add(appenderAlive);
                }

                tbs.add(tbMsg);
                tbs.add(tbAlive);
            }

            @Override
            public void process(Object[] tuple) throws HyracksDataException {
                // vertex Id, msg content List, vertex Id, vertex
                tbMsg.reset();
                tbAlive.reset();

                vertex = (Vertex) tuple[3];
                vertex.setOutputWriters(writers);
                vertex.setOutputAppenders(appenders);
                vertex.setOutputTupleBuilders(tbs);

                ArrayListWritable msgContentList = (ArrayListWritable) tuple[1];
                msgContentList.reset(msgIterator);

                if (!msgIterator.hasNext() && vertex.isHalted())
                    return;

                try {
                    vertex.compute(msgIterator);
                    vertex.finishCompute();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }

                /**
                 * this partition should not terminate
                 */
                if (terminate && (!vertex.isHalted() || vertex.hasMessage()))
                    terminate = false;
            }

            @Override
            public void close() throws HyracksDataException {
                FrameTupleUtils.flushTuplesFinal(appenderMsg, writerMsg);
                if (pushAlive)
                    FrameTupleUtils.flushTuplesFinal(appenderAlive, writerAlive);
                if (!terminate) {
                    writeOutTerminationState();
                }
            }

            private void writeOutTerminationState() throws HyracksDataException {
                try {
                    tbTerminate.getDataOutput().writeLong(0);
                    tbTerminate.addFieldEndOffset();
                    appenderTerminate.append(tbTerminate.getFieldEndOffsets(), tbTerminate.getByteArray(), 0,
                            tbTerminate.getSize());
                    FrameTupleUtils.flushTuplesFinal(appenderTerminate, writerTerminate);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void update(ITupleReference tupleRef) throws HyracksDataException {
                try {
                    if (vertex != null && vertex.hasUpdate()) {
                        int fieldCount = tupleRef.getFieldCount();
                        for (int i = 1; i < fieldCount; i++) {
                            byte[] data = tupleRef.getFieldData(i);
                            int offset = tupleRef.getFieldStart(i);
                            bbos.setByteArray(data, offset);
                            vertex.write(output);
                        }
                    }
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }
}
