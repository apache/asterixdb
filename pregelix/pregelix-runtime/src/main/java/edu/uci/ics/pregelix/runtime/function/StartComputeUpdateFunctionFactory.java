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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.pregelix.api.graph.GlobalAggregator;
import edu.uci.ics.pregelix.api.graph.MsgList;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.ArrayListWritable.ArrayIterator;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.api.util.FrameTupleUtils;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IUpdateFunction;
import edu.uci.ics.pregelix.dataflow.std.base.IUpdateFunctionFactory;
import edu.uci.ics.pregelix.dataflow.util.ResetableByteArrayOutputStream;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class StartComputeUpdateFunctionFactory implements IUpdateFunctionFactory {
    private static final long serialVersionUID = 1L;
    private final IConfigurationFactory confFactory;

    public StartComputeUpdateFunctionFactory(IConfigurationFactory confFactory) {
        this.confFactory = confFactory;
    }

    @Override
    public IUpdateFunction createFunction() {
        return new IUpdateFunction() {
            // for writing intermediate data
            private final ArrayTupleBuilder tbMsg = new ArrayTupleBuilder(2);
            private final ArrayTupleBuilder tbAlive = new ArrayTupleBuilder(2);
            private final ArrayTupleBuilder tbTerminate = new ArrayTupleBuilder(1);
            private final ArrayTupleBuilder tbGlobalAggregate = new ArrayTupleBuilder(1);

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
            private IFrameWriter writerGlobalAggregate;
            private FrameTupleAppender appenderGlobalAggregate;
            private ByteBuffer bufferGlobalAggregate;
            private GlobalAggregator aggregator;

            //for writing out the global aggregate
            private IFrameWriter writerTerminate;
            private FrameTupleAppender appenderTerminate;
            private ByteBuffer bufferTerminate;
            private boolean terminate = true;

            // dummy empty msgList
            private MsgList msgList = new MsgList();
            private ArrayIterator msgIterator = new ArrayIterator();

            private Vertex vertex;
            private ResetableByteArrayOutputStream bbos = new ResetableByteArrayOutputStream();
            private DataOutput output = new DataOutputStream(bbos);

            private final List<IFrameWriter> writers = new ArrayList<IFrameWriter>();
            private final List<FrameTupleAppender> appenders = new ArrayList<FrameTupleAppender>();
            private final List<ArrayTupleBuilder> tbs = new ArrayList<ArrayTupleBuilder>();
            private Configuration conf;

            @Override
            public void open(IHyracksTaskContext ctx, RecordDescriptor rd, IFrameWriter... writers)
                    throws HyracksDataException {
                this.conf = confFactory.createConfiguration();
                this.aggregator = BspUtils.createGlobalAggregator(conf);
                this.aggregator.init();

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

                this.writerGlobalAggregate = writers[2];
                this.bufferGlobalAggregate = ctx.allocateFrame();
                this.appenderGlobalAggregate = new FrameTupleAppender(ctx.getFrameSize());
                this.appenderGlobalAggregate.reset(bufferGlobalAggregate, true);

                if (writers.length > 3) {
                    this.writerAlive = writers[3];
                    this.bufferAlive = ctx.allocateFrame();
                    this.appenderAlive = new FrameTupleAppender(ctx.getFrameSize());
                    this.appenderAlive.reset(bufferAlive, true);
                    this.pushAlive = true;
                    this.writers.add(writerAlive);
                    this.appenders.add(appenderAlive);
                }
                msgList.reset(msgIterator);

                tbs.add(tbMsg);
                tbs.add(tbAlive);
            }

            @Override
            public void process(Object[] tuple) throws HyracksDataException {
                // vertex Id, vertex
                tbMsg.reset();
                tbAlive.reset();

                vertex = (Vertex) tuple[1];
                vertex.setOutputWriters(writers);
                vertex.setOutputAppenders(appenders);
                vertex.setOutputTupleBuilders(tbs);

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

                /**
                 * call the global aggregator
                 */
                aggregator.step(vertex);
            }

            @Override
            public void close() throws HyracksDataException {
                FrameTupleUtils.flushTuplesFinal(appenderMsg, writerMsg);
                if (pushAlive)
                    FrameTupleUtils.flushTuplesFinal(appenderAlive, writerAlive);
                if (!terminate) {
                    writeOutTerminationState();
                }

                /** write out global aggregate value */
                writeOutGlobalAggregate();
            }

            private void writeOutGlobalAggregate() throws HyracksDataException {
                try {
                    /**
                     * get partial aggregate result and flush to the final aggregator
                     */
                    Writable agg = aggregator.finishPartial();
                    agg.write(tbGlobalAggregate.getDataOutput());
                    tbGlobalAggregate.addFieldEndOffset();
                    appenderGlobalAggregate.append(tbGlobalAggregate.getFieldEndOffsets(),
                            tbGlobalAggregate.getByteArray(), 0, tbGlobalAggregate.getSize());
                    FrameTupleUtils.flushTuplesFinal(appenderGlobalAggregate, writerGlobalAggregate);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
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
            public void update(ITupleReference tupleRef, ArrayTupleBuilder cloneUpdateTb) throws HyracksDataException {
                try {
                    if (vertex != null && vertex.hasUpdate()) {
                        if (!BspUtils.getDynamicVertexValueSize(conf)) {
                            //in-place update
                            int fieldCount = tupleRef.getFieldCount();
                            for (int i = 1; i < fieldCount; i++) {
                                byte[] data = tupleRef.getFieldData(i);
                                int offset = tupleRef.getFieldStart(i);
                                bbos.setByteArray(data, offset);
                                vertex.write(output);
                            }
                        } else {
                            //write the vertex id
                            DataOutput tbOutput = cloneUpdateTb.getDataOutput();
                            vertex.getVertexId().write(tbOutput);
                            cloneUpdateTb.addFieldEndOffset();

                            //write the vertex value
                            vertex.write(tbOutput);
                            cloneUpdateTb.addFieldEndOffset();
                        }
                    }
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }
}
