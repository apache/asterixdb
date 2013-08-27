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

package edu.uci.ics.pregelix.dataflow;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.pregelix.api.graph.GlobalAggregator;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRecordDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;

public class FinalAggregateOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final IConfigurationFactory confFactory;
    private final String jobId;
    private final IRecordDescriptorFactory inputRdFactory;

    public FinalAggregateOperatorDescriptor(JobSpecification spec, IConfigurationFactory confFactory,
            IRecordDescriptorFactory inputRdFactory, String jobId) {
        super(spec, 1, 0);
        this.confFactory = confFactory;
        this.jobId = jobId;
        this.inputRdFactory = inputRdFactory;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            private Configuration conf = confFactory.createConfiguration(ctx);
            @SuppressWarnings("rawtypes")
            private GlobalAggregator aggregator = BspUtils.createGlobalAggregator(conf);
            private FrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(),
                    inputRdFactory.createRecordDescriptor(ctx));
            private ByteBufferInputStream inputStream = new ByteBufferInputStream();
            private DataInput input = new DataInputStream(inputStream);
            private Writable partialAggregateValue = BspUtils.createFinalAggregateValue(conf);

            @Override
            public void open() throws HyracksDataException {
                aggregator.init();
            }

            @SuppressWarnings("unchecked")
            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                accessor.reset(buffer);
                int tupleCount = accessor.getTupleCount();
                try {
                    for (int i = 0; i < tupleCount; i++) {
                        int start = accessor.getFieldSlotsLength() + accessor.getTupleStartOffset(i)
                                + accessor.getFieldStartOffset(i, 0);
                        inputStream.setByteBuffer(buffer, start);
                        partialAggregateValue.readFields(input);
                        aggregator.step(partialAggregateValue);
                    }
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void fail() throws HyracksDataException {

            }

            @SuppressWarnings("unchecked")
            @Override
            public void close() throws HyracksDataException {
                try {
                    // iterate over hdfs spilled aggregates
                    FileSystem dfs = FileSystem.get(conf);
                    String spillingDir = BspUtils.getGlobalAggregateSpillingDirName(conf, Vertex.getSuperstep());
                    FileStatus[] files = dfs.listStatus(new Path(spillingDir));
                    if (files != null) {
                        // goes into this branch only when there are spilled files
                        for (int i = 0; i < files.length; i++) {
                            FileStatus file = files[i];
                            DataInput dis = dfs.open(file.getPath());
                            partialAggregateValue.readFields(dis);
                            aggregator.step(partialAggregateValue);
                        }
                    }
                    Writable finalAggregateValue = aggregator.finishFinal();
                    IterationUtils.writeGlobalAggregateValue(conf, jobId, finalAggregateValue);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

        };
    }
}
