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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    @SuppressWarnings("rawtypes")
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            private Configuration conf = confFactory.createConfiguration(ctx);
            private List<GlobalAggregator> aggregators = BspUtils.createGlobalAggregators(conf);
            private List<String> aggregateClassNames = Arrays.asList(BspUtils.getGlobalAggregatorClassNames(conf));
            private FrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(),
                    inputRdFactory.createRecordDescriptor(ctx));
            private ByteBufferInputStream inputStream = new ByteBufferInputStream();
            private DataInput input = new DataInputStream(inputStream);
            private List<Writable> partialAggregateValues = BspUtils.createFinalAggregateValues(conf);

            @Override
            public void open() throws HyracksDataException {
                for (GlobalAggregator aggregator : aggregators) {
                    aggregator.init();
                }
            }

            @SuppressWarnings("unchecked")
            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                accessor.reset(buffer);
                int tupleCount = accessor.getTupleCount();
                try {
                    for (int i = 0; i < tupleCount; i++) {
                        // iterate over all the aggregators
                        for (int j = 0; j < partialAggregateValues.size(); j++) {
                            int start = accessor.getFieldSlotsLength() + accessor.getTupleStartOffset(i)
                                    + accessor.getFieldStartOffset(i, j);
                            inputStream.setByteBuffer(buffer, start);
                            partialAggregateValues.get(j).readFields(input);
                            aggregators.get(j).step(partialAggregateValues.get(j));
                        }
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
                    List<Writable> aggValues = new ArrayList<Writable>();
                    // iterate over hdfs spilled aggregates
                    FileSystem dfs = FileSystem.get(conf);
                    String spillingDir = BspUtils.getGlobalAggregateSpillingDirName(conf,
                            IterationUtils.getSuperstep(BspUtils.getJobId(conf), ctx));
                    FileStatus[] files = dfs.listStatus(new Path(spillingDir));
                    if (files != null) {
                        // goes into this branch only when there are spilled files
                        for (int i = 0; i < files.length; i++) {
                            FileStatus file = files[i];
                            DataInput dis = dfs.open(file.getPath());
                            for (int j = 0; j < partialAggregateValues.size(); j++) {
                                GlobalAggregator aggregator = aggregators.get(j);
                                Writable partialAggregateValue = partialAggregateValues.get(j);
                                partialAggregateValue.readFields(dis);
                                aggregator.step(partialAggregateValue);
                            }
                        }
                    }
                    for (int j = 0; j < partialAggregateValues.size(); j++) {
                        GlobalAggregator aggregator = aggregators.get(j);
                        Writable finalAggregateValue = aggregator.finishFinal();
                        aggValues.add(finalAggregateValue);
                    }
                    IterationUtils.writeGlobalAggregateValue(conf, jobId, aggregateClassNames, aggValues);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

        };
    }
}
