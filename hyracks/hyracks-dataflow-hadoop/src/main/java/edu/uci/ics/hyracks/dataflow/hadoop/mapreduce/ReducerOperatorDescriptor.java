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
package edu.uci.ics.hyracks.dataflow.hadoop.mapreduce;

import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

public class ReducerOperatorDescriptor<K2 extends Writable, V2 extends Writable, K3 extends Writable, V3 extends Writable>
        extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final int jobId;

    private MarshalledWritable<Configuration> mConfig;

    public ReducerOperatorDescriptor(IOperatorDescriptorRegistry spec, int jobId, MarshalledWritable<Configuration> mConfig) {
        super(spec, 1, 0);
        this.jobId = jobId;
        this.mConfig = mConfig;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        final HadoopHelper helper = new HadoopHelper(mConfig);
        final Reducer<K2, V2, K3, V3> reducer = helper.getReducer();
        final RecordDescriptor recordDescriptor = helper.getMapOutputRecordDescriptor();
        final int[] groupFields = helper.getSortFields();
        IBinaryComparatorFactory[] groupingComparators = helper.getGroupingComparatorFactories();

        final TaskAttemptID taId = new TaskAttemptID("foo", jobId, false, partition, 0);
        final TaskAttemptContext taskAttemptContext = helper.createTaskAttemptContext(taId);
        final RecordWriter recordWriter;
        try {
            recordWriter = helper.getOutputFormat().getRecordWriter(taskAttemptContext);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }

        final ReduceWriter<K2, V2, K3, V3> rw = new ReduceWriter<K2, V2, K3, V3>(ctx, helper, groupFields,
                groupingComparators, recordDescriptor, reducer, recordWriter, taId, taskAttemptContext);

        return new AbstractUnaryInputSinkOperatorNodePushable() {
            @Override
            public void open() throws HyracksDataException {
                rw.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                rw.nextFrame(buffer);
            }

            @Override
            public void close() throws HyracksDataException {
                rw.close();
            }

            @Override
            public void fail() throws HyracksDataException {
            }
        };
    }
}