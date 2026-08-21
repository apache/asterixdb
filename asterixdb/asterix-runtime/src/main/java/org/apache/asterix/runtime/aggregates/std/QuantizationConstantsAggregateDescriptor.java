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
package org.apache.asterix.runtime.aggregates.std;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABinary;
import org.apache.asterix.om.base.AMutableBinary;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.aggregates.base.AbstractAggregateFunctionDynamicDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.BufferSerDeUtil;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;

/**
 * Aggregate function to compute quantization constants (minQ, maxQ, alpha) from scalar values.
 *
 * Local aggregate: Collects all scalar double values.
 * Global aggregate: Sorts collected values, computes quantiles using confidence interval,
 * and calculates alpha based on bits parameter.
 */
public class QuantizationConstantsAggregateDescriptor extends AbstractAggregateFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private float confidenceInterval;
    private int bits;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new QuantizationConstantsAggregateDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_ARGUMENT_TYPE;
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.QUANTIZATION_CONSTANTS;
    }

    @Override
    public void setImmutableStates(Object... states) {
        confidenceInterval = (float) states[0];
        bits = (int) states[1];
    }

    @Override
    public IAggregateEvaluatorFactory createAggregateEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IAggregateEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IAggregateEvaluator createAggregateEvaluator(final IEvaluatorContext ctx)
                    throws HyracksDataException {
                return new QuantizationConstantsFunction(args, ctx, confidenceInterval, bits, sourceLoc);
            }
        };
    }

    private static class QuantizationConstantsFunction extends AbstractAggregateFunction {
        @SuppressWarnings("unchecked")
        private ISerializerDeserializer<ABinary> binarySerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABINARY);
        private final AMutableBinary binary = new AMutableBinary(null, 0, 0);
        private final ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        private final ArrayBackedValueStorage valuesBits = new ArrayBackedValueStorage();
        private final IPointable input = new VoidPointable();
        private final ByteArrayPointable valuesPointable = new ByteArrayPointable();
        private final IScalarEvaluator scalarValueEval;
        private final IEvaluatorContext context;
        private final float confidenceInterval;
        private final int bits;
        private final List<Double> localValues = new ArrayList<>();
        private final List<Double> finalValues = new ArrayList<>();
        private boolean isWarned = false;

        private QuantizationConstantsFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
                float confidenceInterval, int bits, SourceLocation sourceLocation) throws HyracksDataException {
            super(sourceLocation);
            this.scalarValueEval = args[0].createScalarEvaluator(context);
            this.context = context;
            this.confidenceInterval = confidenceInterval;
            this.bits = bits;
        }

        @Override
        public void init() throws HyracksDataException {
            localValues.clear();
            finalValues.clear();
            isWarned = false;
        }

        @Override
        public void step(IFrameTupleReference tuple) throws HyracksDataException {
            scalarValueEval.evaluate(tuple, input);
            byte[] data = input.getByteArray();
            int offset = input.getStartOffset();

            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);

            if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL || typeTag == ATypeTag.SYSTEM_NULL) {
                return;
            }

            if (typeTag == ATypeTag.BINARY) {
                deserializeBinaryValues(data, offset);
            } else {
                double value = extractNumericValue(data, offset, typeTag);
                if (!Double.isNaN(value)) {
                    localValues.add(value);
                }
            }
        }

        private void deserializeBinaryValues(byte[] data, int offset) throws HyracksDataException {
            int binaryLength = input.getLength() - 1;
            valuesPointable.set(data, offset + 1, binaryLength);

            byte[] valuesBytes = valuesPointable.getByteArray();
            int contentStartOffset = valuesPointable.getContentStartOffset();
            int contentLength = valuesPointable.getContentLength();

            if (contentLength < Integer.BYTES) {
                throw new RuntimeDataException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                        "Quantization constants are too short: expected at least " + Integer.BYTES
                                + " bytes for numValues, got " + contentLength);
            }

            if (contentStartOffset + Integer.BYTES > valuesBytes.length) {
                throw new RuntimeDataException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                        "Quantization constants are out of bounds: reading numValues at offset " + contentStartOffset
                                + ", but the array length is " + valuesBytes.length);
            }

            int numValues = IntegerPointable.getInteger(valuesBytes, contentStartOffset);

            if (numValues < 0) {
                throw new RuntimeDataException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                        "Quantization constants declare a negative value count: " + numValues);
            }
            if (numValues > 10000000) {
                throw new RuntimeDataException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                        "Quantization constants declare an implausibly large value count: " + numValues);
            }

            long expectedBytes = (long) Integer.BYTES + ((long) numValues * (long) Double.BYTES);
            if (expectedBytes > Integer.MAX_VALUE) {
                throw new RuntimeDataException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                        "Quantization constants require " + expectedBytes + " bytes, which exceeds the maximum "
                                + Integer.MAX_VALUE);
            }
            int expectedBytesInt = (int) expectedBytes;

            if (expectedBytesInt > contentLength) {
                throw new RuntimeDataException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                        "Quantization constants are too short: expected " + expectedBytesInt + " bytes, got "
                                + contentLength + " for " + numValues + " values");
            }

            int lastByteOffset = contentStartOffset + expectedBytesInt - 1;
            if (lastByteOffset >= valuesBytes.length) {
                throw new RuntimeDataException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                        "Quantization constants are out of bounds: reading up to offset " + lastByteOffset
                                + ", but the array length is " + valuesBytes.length);
            }

            int pointer = contentStartOffset + Integer.BYTES;
            for (int i = 0; i < numValues; i++) {
                if (pointer + Double.BYTES > contentStartOffset + contentLength) {
                    throw new RuntimeDataException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                            "Quantization constants are out of bounds: reading value #" + (i + 1) + " at offset "
                                    + pointer + ", but the content ends at " + (contentStartOffset + contentLength));
                }

                if (pointer + Double.BYTES > valuesBytes.length) {
                    throw new RuntimeDataException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                            "Quantization constants are out of bounds: reading value #" + (i + 1) + " at offset "
                                    + pointer + ", but the array length is " + valuesBytes.length);
                }

                double value = BufferSerDeUtil.getDouble(valuesBytes, pointer);
                pointer += Double.BYTES;
                finalValues.add(value);
            }
        }

        private double extractNumericValue(byte[] data, int offset, ATypeTag typeTag) {
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
                    if (!isWarned) {
                        isWarned = true;
                        ExceptionUtil.warnUnsupportedType(context, sourceLoc, getIdentifier().getName(), typeTag);
                    }
                    return Double.NaN;
            }
        }

        @Override
        public void finish(IPointable result) throws HyracksDataException {
            storage.reset();
            try {
                if (!localValues.isEmpty()) {
                    valuesBits.reset();
                    IntegerSerializerDeserializer.write(localValues.size(), valuesBits.getDataOutput());
                    for (Double value : localValues) {
                        DoubleSerializerDeserializer.write(value, valuesBits.getDataOutput());
                    }
                    binary.setValue(valuesBits.getByteArray(), valuesBits.getStartOffset(), valuesBits.getLength());
                    binarySerde.serialize(binary, storage.getDataOutput());
                    result.set(storage);
                } else if (!finalValues.isEmpty()) {
                    Collections.sort(finalValues);

                    float half = (1.0f - confidenceInterval) / 2.0f;
                    int totalCount = finalValues.size();
                    int lowerIdx = (int) Math.floor(half * (totalCount - 1));
                    int upperIdx = (int) Math.ceil((1.0f - half) * (totalCount - 1));

                    lowerIdx = Math.max(0, Math.min(lowerIdx, totalCount - 1));
                    upperIdx = Math.max(0, Math.min(upperIdx, totalCount - 1));

                    float minQ = finalValues.get(lowerIdx).floatValue();
                    float maxQ = finalValues.get(upperIdx).floatValue();

                    double eps = 1e-12;
                    if (maxQ <= minQ + eps) {
                        maxQ = minQ + 1e-6f;
                    }

                    int levels = 1 << bits;
                    float alpha = (levels - 1) / (maxQ - minQ);

                    VTreeQuantizationParams constants =
                            new VTreeQuantizationParams(minQ, maxQ, alpha, confidenceInterval, bits, totalCount);

                    valuesBits.reset();
                    serializeQuantizationConstants(constants, valuesBits.getDataOutput());
                    binary.setValue(valuesBits.getByteArray(), valuesBits.getStartOffset(), valuesBits.getLength());
                    binarySerde.serialize(binary, storage.getDataOutput());
                    result.set(storage);
                } else {
                    storage.getDataOutput().writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
                    result.set(storage);
                }
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }

        private void serializeQuantizationConstants(VTreeQuantizationParams constants, DataOutput out)
                throws IOException {
            // Wire order is fixed (minQ, maxQ, alpha, bits, confidence, sampleCount); the record's component
            // order differs (confidenceInterval before bits), so read via accessors but keep this order.
            FloatSerializerDeserializer.write(constants.minQuantile(), out);
            FloatSerializerDeserializer.write(constants.maxQuantile(), out);
            FloatSerializerDeserializer.write(constants.alpha(), out);
            IntegerSerializerDeserializer.write(constants.bits(), out);
            FloatSerializerDeserializer.write(constants.confidenceInterval(), out);
            IntegerSerializerDeserializer.write(constants.sampleCount(), out);
        }

        @Override
        public void finishPartial(IPointable result) throws HyracksDataException {
            storage.reset();
            try {
                if (!localValues.isEmpty()) {
                    valuesBits.reset();
                    IntegerSerializerDeserializer.write(localValues.size(), valuesBits.getDataOutput());
                    for (Double value : localValues) {
                        DoubleSerializerDeserializer.write(value, valuesBits.getDataOutput());
                    }
                    binary.setValue(valuesBits.getByteArray(), valuesBits.getStartOffset(), valuesBits.getLength());
                    binarySerde.serialize(binary, storage.getDataOutput());
                    result.set(storage);
                } else if (!finalValues.isEmpty()) {
                    valuesBits.reset();
                    IntegerSerializerDeserializer.write(finalValues.size(), valuesBits.getDataOutput());
                    for (Double value : finalValues) {
                        DoubleSerializerDeserializer.write(value, valuesBits.getDataOutput());
                    }
                    binary.setValue(valuesBits.getByteArray(), valuesBits.getStartOffset(), valuesBits.getLength());
                    binarySerde.serialize(binary, storage.getDataOutput());
                    result.set(storage);
                } else {
                    storage.getDataOutput().writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
                    result.set(storage);
                }
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }

        private FunctionIdentifier getIdentifier() {
            return BuiltinFunctions.QUANTIZATION_CONSTANTS;
        }
    }
}
