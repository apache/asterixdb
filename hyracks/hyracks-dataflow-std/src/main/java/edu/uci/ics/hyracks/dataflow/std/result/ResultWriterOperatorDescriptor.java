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
package edu.uci.ics.hyracks.dataflow.std.result;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataset.IDatasetPartitionManager;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

public class ResultWriterOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    public ResultWriterOperatorDescriptor(IOperatorDescriptorRegistry spec) {
        super(spec, 1, 0);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions) {
        final JobId jobId = ctx.getJobletContext().getJobId();
        final IDatasetPartitionManager dpm = ctx.getDatasetPartitionManager();

        return new AbstractUnaryInputSinkOperatorNodePushable() {
            IFrameWriter datasetPartitionWriter;

            @Override
            public void open() throws HyracksDataException {
                datasetPartitionWriter = dpm.createDatasetPartitionWriter(jobId, partition, nPartitions);
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                datasetPartitionWriter.nextFrame(buffer);
            }

            @Override
            public void fail() throws HyracksDataException {
                datasetPartitionWriter.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                datasetPartitionWriter.close();
            }
        };
    }
}