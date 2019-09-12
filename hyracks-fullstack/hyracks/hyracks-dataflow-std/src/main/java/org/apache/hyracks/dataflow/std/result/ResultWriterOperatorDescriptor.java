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
package org.apache.hyracks.dataflow.std.result;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.IResultSerializer;
import org.apache.hyracks.api.dataflow.value.IResultSerializerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.result.IResultMetadata;
import org.apache.hyracks.api.result.IResultPartitionManager;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.dataflow.common.comm.io.FrameOutputStream;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

public class ResultWriterOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final ResultSetId rsId;

    private final IResultMetadata metadata;

    private final boolean asyncMode;

    private final IResultSerializerFactory resultSerializerFactory;
    private final long maxReads;

    public ResultWriterOperatorDescriptor(IOperatorDescriptorRegistry spec, ResultSetId rsId, IResultMetadata metadata,
            boolean asyncMode, IResultSerializerFactory resultSerializerFactory, long maxReads) throws IOException {
        super(spec, 1, 0);
        this.rsId = rsId;
        this.metadata = metadata;
        this.asyncMode = asyncMode;
        this.resultSerializerFactory = resultSerializerFactory;
        this.maxReads = maxReads;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
            throws HyracksDataException {
        final IResultPartitionManager resultPartitionManager = ctx.getResultPartitionManager();

        final IFrame frame = new VSizeFrame(ctx);

        final FrameOutputStream frameOutputStream = new FrameOutputStream(ctx.getInitialFrameSize());
        frameOutputStream.reset(frame, true);
        PrintStream printStream = new PrintStream(frameOutputStream);

        final RecordDescriptor outRecordDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        final IResultSerializer resultSerializer =
                resultSerializerFactory.createResultSerializer(outRecordDesc, printStream);

        final FrameTupleAccessor frameTupleAccessor = new FrameTupleAccessor(outRecordDesc);

        return new AbstractUnaryInputSinkOperatorNodePushable() {
            private IFrameWriter resultPartitionWriter;
            private boolean failed = false;

            @Override
            public void open() throws HyracksDataException {
                try {
                    resultPartitionWriter = resultPartitionManager.createResultPartitionWriter(ctx, rsId, metadata,
                            asyncMode, partition, nPartitions, maxReads);
                    resultPartitionWriter.open();
                    resultSerializer.init();
                } catch (HyracksException e) {
                    throw HyracksDataException.create(e);
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                frameTupleAccessor.reset(buffer);
                for (int tIndex = 0; tIndex < frameTupleAccessor.getTupleCount(); tIndex++) {
                    resultSerializer.appendTuple(frameTupleAccessor, tIndex);
                    if (!frameOutputStream.appendTuple()) {
                        frameOutputStream.flush(resultPartitionWriter);

                        resultSerializer.appendTuple(frameTupleAccessor, tIndex);
                        frameOutputStream.appendTuple();
                    }
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                failed = true;
                if (resultPartitionWriter != null) {
                    resultPartitionWriter.fail();
                }
            }

            @Override
            public void close() throws HyracksDataException {
                if (resultPartitionWriter != null) {
                    try {
                        if (!failed && frameOutputStream.getTupleCount() > 0) {
                            frameOutputStream.flush(resultPartitionWriter);
                        }
                    } catch (Exception e) {
                        resultPartitionWriter.fail();
                        throw e;
                    } finally {
                        resultPartitionWriter.close();
                    }
                }
            }

            @Override
            public String toString() {
                StringBuilder sb = new StringBuilder();
                sb.append("{ ");
                sb.append("\"rsId\": \"").append(rsId).append("\", ");
                sb.append("\"metadata\": ").append(metadata).append(", ");
                sb.append("\"asyncMode\": ").append(asyncMode).append(", ");
                sb.append("\"maxReads\": ").append(maxReads).append(" }");
                return sb.toString();
            }

            @Override
            public String getDisplayName() {
                return "Result Writer";
            }
        };
    }
}
