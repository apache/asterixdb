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
package org.apache.hyracks.storage.am.lsm.vector.dataflow;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexBuilder;
import org.apache.hyracks.storage.am.common.api.IIndexBuilderFactory;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;

/**
 * Operator descriptor for creating a quantized vector index.
 *
 * Input: a single tuple containing the serialized quantization parameters
 *        (minQuantile, maxQuantile, alpha, bits, confidenceInterval, sampleCount).
 * Output: none (sink). On close() the underlying {@link IIndexBuilder}s are built,
 *         propagating the extracted quantization parameters to each storage partition.
 */
public class QuantizedIndexCreateOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final IIndexBuilderFactory[][] indexBuilderFactories;
    private final int[][] partitionsMap;

    public QuantizedIndexCreateOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexBuilderFactory[][] indexBuilderFactories, int[][] partitionsMap, RecordDescriptor recordDesc) {
        super(spec, 1, 0);
        this.indexBuilderFactories = indexBuilderFactories;
        this.partitionsMap = partitionsMap;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        int[] storagePartitions = partitionsMap[partition];
        IIndexBuilderFactory[] partitionIndexBuilderFactories = indexBuilderFactories[partition];
        IIndexBuilder[] indexBuilders = new IIndexBuilder[storagePartitions.length];
        for (int i = 0; i < storagePartitions.length; i++) {
            indexBuilders[i] = partitionIndexBuilderFactories[i].create(ctx, storagePartitions[i]);
        }
        RecordDescriptor inputRecordDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new QuantizedIndexCreateOperatorNodePushable(ctx, indexBuilders, inputRecordDesc);
    }

    private static class QuantizedIndexCreateOperatorNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
        private final IIndexBuilder[] indexBuilders;
        private final FrameTupleAccessor tupleAccessor;
        private final FrameTupleReference tupleHelper;
        private VTreeQuantizationParams quantizationParams;
        private boolean failed;

        public QuantizedIndexCreateOperatorNodePushable(IHyracksTaskContext ctx, IIndexBuilder[] indexBuilders,
                RecordDescriptor inputRecordDesc) {
            this.indexBuilders = indexBuilders;
            this.tupleAccessor = new FrameTupleAccessor(inputRecordDesc);
            this.tupleHelper = new FrameTupleReference();
        }

        @Override
        public void open() throws HyracksDataException {
            // No-op: index builders are constructed eagerly; build() is deferred until close().
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            tupleAccessor.reset(buffer);
            int tupleCount = tupleAccessor.getTupleCount();
            if (tupleCount == 0) {
                return;
            }
            // Exactly one quantization-params tuple is expected. Fail loudly on a second tuple/frame or a
            // multi-tuple frame rather than silently dropping the extras (a wrong-input hazard).
            if (quantizationParams != null) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                        "QuantizedIndexCreateOperator received more than one quantization-params tuple");
            }
            if (tupleCount != 1) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                        "QuantizedIndexCreateOperator expected a single quantization-params tuple but got "
                                + tupleCount);
            }
            tupleHelper.reset(tupleAccessor, 0);
            quantizationParams = extractQuantizationParams(tupleHelper);
            for (IIndexBuilder indexBuilder : indexBuilders) {
                if (indexBuilder instanceof QuantizedIndexBuilder) {
                    ((QuantizedIndexBuilder) indexBuilder).setQuantizationParameters(quantizationParams);
                }
            }
        }

        // Six big-endian 4-byte values: minQuantile, maxQuantile, alpha (floats), bits (int),
        // confidenceInterval (float), sampleCount (int).
        private static final int PARAMS_PAYLOAD_BYTES = 6 * Integer.BYTES;

        private VTreeQuantizationParams extractQuantizationParams(FrameTupleReference tuple)
                throws HyracksDataException {
            byte[] data = tuple.getFieldData(0);
            int start = tuple.getFieldStart(0);
            int length = tuple.getFieldLength(0);

            // Skip the 1-byte ADM type tag, then the ByteArrayPointable's varlen length prefix.
            ByteArrayPointable ptr = new ByteArrayPointable();
            ptr.set(data, start + 1, length - 1);
            int contentLength = ptr.getContentLength();
            // The global aggregate emits either the full parameter block or nothing, and nothing means the
            // sample yielded no usable vector.
            if (contentLength < PARAMS_PAYLOAD_BYTES) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                        "no usable quantization parameters for this vector index (" + contentLength + " of "
                                + PARAMS_PAYLOAD_BYTES + " bytes). The sampled records yielded no usable vector for "
                                + "the indexed field: it may be missing, null or not a list in every sampled record, "
                                + "the dataset may be empty, or no sampled vector may match the dimension the index "
                                + "declares.");
            }

            // Big-endian; DataInputStream is the exact inverse of the DataOutput/ByteBuffer writer and
            // bounds-checks (EOFException) instead of reading past the field.
            try (DataInputStream in = new DataInputStream(
                    new ByteArrayInputStream(ptr.getByteArray(), ptr.getContentStartOffset(), contentLength))) {
                float minQuantile = in.readFloat();
                float maxQuantile = in.readFloat();
                float alpha = in.readFloat();
                int bits = in.readInt();
                float confidenceInterval = in.readFloat();
                int sampleCount = in.readInt();
                return new VTreeQuantizationParams(minQuantile, maxQuantile, alpha, confidenceInterval, bits,
                        sampleCount);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            // A failure upstream (e.g. the sampler producing the quantization constants) must not leave a
            // half-built index: mark failed so close() skips build() rather than building from a failed
            // or partial pipeline.
            failed = true;
        }

        @Override
        public void close() throws HyracksDataException {
            if (failed) {
                return; // do not build from a failed pipeline; let the original failure propagate
            }
            if (quantizationParams != null) {
                for (IIndexBuilder indexBuilder : indexBuilders) {
                    indexBuilder.build();
                }
            } else {
                throw HyracksDataException.create(ErrorCode.VECTOR_INDEX_BUILD_FAILED,
                        "No quantization constants were received");
            }
        }
    }
}
