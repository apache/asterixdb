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
package org.apache.hyracks.dataflow.hadoop.mapreduce;

import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

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