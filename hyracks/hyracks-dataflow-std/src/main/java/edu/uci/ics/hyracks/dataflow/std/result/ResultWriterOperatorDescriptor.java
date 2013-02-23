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

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IResultSerializedAppender;
import edu.uci.ics.hyracks.api.dataflow.value.IResultSerializedAppenderFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataset.IDatasetPartitionManager;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameOutputStream;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

public class ResultWriterOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final ResultSetId rsId;

    private final boolean ordered;

    private final RecordDescriptor recordDescriptor;

    private final IResultSerializedAppenderFactory resultSerializedAppenderFactory;

    private final byte[] serializedRecordDescriptor;

    public ResultWriterOperatorDescriptor(IOperatorDescriptorRegistry spec, ResultSetId rsId, boolean ordered,
            RecordDescriptor recordDescriptor, IResultSerializedAppenderFactory resultSerializedAppenderFactory)
            throws IOException {
        super(spec, 1, 0);
        this.rsId = rsId;
        this.ordered = ordered;
        this.recordDescriptor = recordDescriptor;
        this.resultSerializedAppenderFactory = resultSerializedAppenderFactory;
        this.serializedRecordDescriptor = JavaSerializationUtils.serialize(recordDescriptor);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions) {
        final IDatasetPartitionManager dpm = ctx.getDatasetPartitionManager();

        final ByteBuffer tempBuffer = ctx.allocateFrame();
        tempBuffer.clear();

        final FrameTupleAppender frameTupleAppender = new FrameTupleAppender(ctx.getFrameSize());
        frameTupleAppender.reset(tempBuffer, true);

        final FrameOutputStream frameOutputStream = new FrameOutputStream(frameTupleAppender);

        final IResultSerializedAppender resultSerializedAppender = resultSerializedAppenderFactory
                .createResultSerializer(frameOutputStream);

        final FrameTupleAccessor frameTupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);

        return new AbstractUnaryInputSinkOperatorNodePushable() {
            IFrameWriter datasetPartitionWriter;

            @Override
            public void open() throws HyracksDataException {
                try {
                    datasetPartitionWriter = dpm.createDatasetPartitionWriter(ctx, rsId, ordered,
                            serializedRecordDescriptor, partition, nPartitions);
                    datasetPartitionWriter.open();
                } catch (HyracksException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                frameTupleAccessor.reset(buffer);
                for (int tIndex = 0; tIndex < frameTupleAccessor.getTupleCount(); tIndex++) {
                    if (!resultSerializedAppender.appendTuple(frameTupleAccessor, tIndex)) {
                        datasetPartitionWriter.nextFrame(tempBuffer);
                        frameTupleAppender.reset(tempBuffer, true);

                        /* TODO(madhusudancs): This works under the assumption that no single JSON-ified record is
                         * longer than the buffer size.
                         */
                        resultSerializedAppender.appendTuple(frameTupleAccessor, tIndex);
                    }
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                datasetPartitionWriter.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                if (frameTupleAppender.getTupleCount() > 0) {
                    datasetPartitionWriter.nextFrame(tempBuffer);
                    frameTupleAppender.reset(tempBuffer, true);
                }
                datasetPartitionWriter.close();
            }
        };
    }
}