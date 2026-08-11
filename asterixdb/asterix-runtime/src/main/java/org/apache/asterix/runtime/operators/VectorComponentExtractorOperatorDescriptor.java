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
package org.apache.asterix.runtime.operators;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

/**
 * Operator that extracts a vector field from input tuples and flattens its components
 * into individual scalar double values, emitting one tuple per component.
 * This operator:
 * 1. Extracts vector field from input tuple using IScalarEvaluator
 * 2. Iterates through array/list elements using ListAccessor
 * 3. Coerces each component to double (following KMeansUtils pattern)
 * 4. Emits one tuple per component value with type tag (tag | value format)
 */
public class VectorComponentExtractorOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final IScalarEvaluatorFactory vectorFieldEvalFactory;
    private final RecordDescriptor inputRecDesc;
    private final int vectorDimension;

    public VectorComponentExtractorOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IScalarEvaluatorFactory vectorFieldEvalFactory, RecordDescriptor inputRecDesc,
            RecordDescriptor outputRecDesc, int vectorDimension) {
        super(spec, 1, 1);
        this.vectorFieldEvalFactory = vectorFieldEvalFactory;
        this.inputRecDesc = inputRecDesc;
        this.outRecDescs[0] = outputRecDesc;
        this.vectorDimension = vectorDimension;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        RecordDescriptor inputRecordDescriptor = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new VectorComponentExtractorNodePushable(ctx, inputRecordDescriptor, vectorFieldEvalFactory);
    }

    private class VectorComponentExtractorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
        private final IHyracksTaskContext ctx;
        private final FrameTupleAccessor frameTupleAccessor;
        private final FrameTupleReference frameTupleReference;
        private final IScalarEvaluatorFactory vectorFieldEvalFactory;
        private IScalarEvaluator vectorFieldEval;
        private final IPointable vectorFieldValue = new VoidPointable();
        private final ListAccessor listAccessor = new ListAccessor();
        private boolean sawIndexableVector;
        private long skippedVectorCount;
        private int skippedDimension = -1;
        private final IPointable tempVal = new VoidPointable();
        private final ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<ADouble> doubleSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
        private final AMutableDouble aDouble = new AMutableDouble(0);
        private FrameTupleAppender appender;

        private VectorComponentExtractorNodePushable(IHyracksTaskContext ctx, RecordDescriptor inputRecordDescriptor,
                IScalarEvaluatorFactory vectorFieldEvalFactory) {
            this.ctx = ctx;
            this.frameTupleAccessor = new FrameTupleAccessor(inputRecordDescriptor);
            this.frameTupleReference = new FrameTupleReference();
            this.vectorFieldEvalFactory = vectorFieldEvalFactory;
        }

        @Override
        public void open() throws HyracksDataException {
            writer.open();
            appender = new FrameTupleAppender(new VSizeFrame(ctx));
            vectorFieldEval = vectorFieldEvalFactory.createScalarEvaluator(new EvaluatorContext(ctx));
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            frameTupleAccessor.reset(buffer);
            int tupleCount = frameTupleAccessor.getTupleCount();

            for (int i = 0; i < tupleCount; i++) {
                frameTupleReference.reset(frameTupleAccessor, i);

                // Extract vector field from tuple
                vectorFieldEval.evaluate(frameTupleReference, vectorFieldValue);

                // Check if vector field is null/missing
                byte[] data = vectorFieldValue.getByteArray();
                int offset = vectorFieldValue.getStartOffset();
                if (vectorFieldValue.getLength() == 0) {
                    continue;
                }

                ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
                if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL || typeTag == ATypeTag.SYSTEM_NULL) {
                    continue;
                }

                // Check if it's a list type (required for vector)
                if (!typeTag.isListType()) {
                    continue;
                }

                // Iterate through array elements and emit one tuple per component
                listAccessor.reset(data, offset);
                ATypeTag itemTypeTag = listAccessor.getItemType();
                int listSize = listAccessor.size();

                // The bulk load and k-means both skip a vector of another dimension, so its components must
                // not shape the quantization constants either.
                if (listSize != vectorDimension) {
                    skippedVectorCount++;
                    if (skippedDimension < 0) {
                        skippedDimension = listSize;
                    }
                    continue;
                }
                sawIndexableVector = true;

                for (int j = 0; j < listSize; j++) {
                    try {
                        // Get item from list (can throw IOException)
                        listAccessor.getOrWriteItem(j, tempVal, storage);

                        // Extract numeric value (coerce to double)
                        double componentValue = extractNumericValue(tempVal, itemTypeTag);
                        if (Double.isNaN(componentValue)) {
                            continue; // Skip invalid values
                        }

                        // Emit tuple with serialized double value (tag | value format)
                        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);
                        tupleBuilder.reset();

                        // Serialize double value with type tag
                        storage.reset();
                        aDouble.setValue(componentValue);
                        doubleSerde.serialize(aDouble, storage.getDataOutput());

                        tupleBuilder.addField(storage.getByteArray(), storage.getStartOffset(), storage.getLength());

                        // Append tuple to frame (following DataflowUtils pattern)
                        if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                tupleBuilder.getSize())) {
                            // Frame is full, flush and reset
                            FrameUtils.flushFrame(appender.getBuffer(), writer);
                            appender.reset(new VSizeFrame(ctx), true);
                            if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                    tupleBuilder.getSize())) {
                                throw HyracksDataException.create(new IOException("Tuple too large to fit in frame"));
                            }
                        }
                    } catch (IOException e) {
                        throw HyracksDataException.create(e);
                    }
                }
            }
        }

        /**
         * Extracts numeric value from pointable, following KMeansUtils pattern.
         * Coerces all numeric types to double.
         */
        private double extractNumericValue(IPointable pointable, ATypeTag derivedTypeTag) throws HyracksDataException {
            byte[] data = pointable.getByteArray();
            int offset = pointable.getStartOffset();

            if (ATypeHierarchy.getTypeDomain(derivedTypeTag) == ATypeHierarchy.Domain.NUMERIC) {
                double value = getValueFromTag(derivedTypeTag, data, offset);
                return value;
            } else if (derivedTypeTag == ATypeTag.ANY) {
                ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
                double value = getValueFromTag(typeTag, data, offset);
                return value;
            } else {
                return Double.NaN; // Invalid type
            }
        }

        /**
         * Gets numeric value from type tag, following KMeansUtils.getValueFromTag pattern.
         */
        private double getValueFromTag(ATypeTag typeTag, byte[] data, int offset) throws HyracksDataException {
            switch (typeTag) {
                case TINYINT:
                    return AInt8SerializerDeserializer.getByte(data, offset + 1);
                case SMALLINT:
                    return AInt16SerializerDeserializer.getShort(data, offset + 1);
                case INTEGER:
                    return AInt32SerializerDeserializer.getInt(data, offset + 1);
                case BIGINT:
                    return AInt64SerializerDeserializer.getLong(data, offset + 1);
                case FLOAT:
                    return AFloatSerializerDeserializer.getFloat(data, offset + 1);
                case DOUBLE:
                    return ADoubleSerializerDeserializer.getDouble(data, offset + 1);
                default:
                    return Double.NaN;
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            writer.fail();
        }

        /**
         * Rejects a partition that sampled vectors but can index none of them: it would contribute nothing to
         * the index. Raised in this first build job, ahead of any training work.
         */
        private void rejectIfNothingIndexable() throws HyracksDataException {
            if (!sawIndexableVector && skippedVectorCount > 0) {
                throw new RuntimeDataException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                        "the index declares dimension " + vectorDimension + ", but none of the " + skippedVectorCount
                                + " vector(s) sampled in one partition match it (found " + "dimension "
                                + skippedDimension + "). Set the index \"dimension\" parameter to match the data.");
            }
        }

        @Override
        public void close() throws HyracksDataException {
            // A failed writer must still be closed: fail() only moves it to FAILED, and per IFrameWriter
            // close() is the one call allowed from there.
            HyracksDataException failure = null;
            try {
                rejectIfNothingIndexable();
                FrameUtils.flushFrame(appender.getBuffer(), writer);
            } catch (HyracksDataException e) {
                failure = e;
                writer.fail();
            }
            try {
                writer.close();
            } catch (Throwable closeFailure) {
                if (failure == null) {
                    throw closeFailure;
                }
                failure.addSuppressed(closeFailure);
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}
