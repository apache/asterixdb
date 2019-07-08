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
/*
 * Numeric function Round half to even
 * Author : Xiaoyu Ma@UC Irvine
 * 01/30/2012
 */
package org.apache.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.math.BigDecimal;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableFloat;
import org.apache.asterix.om.base.AMutableInt16;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AMutableInt8;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@MissingNullInOutFunction
public class NumericTruncDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new NumericTruncDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.NUMERIC_TRUNC;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new NumericTruncEvaluator(args, ctx);
            }
        };
    }

    class NumericTruncEvaluator implements IScalarEvaluator {
        private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
        private final DataOutput out = resultStorage.getDataOutput();
        private final IPointable argValue = new VoidPointable();
        private final IPointable argPrecision = new VoidPointable();
        private final IScalarEvaluator eval;
        private final IScalarEvaluator precision;
        private final AMutableDouble aDouble = new AMutableDouble(0);
        private final AMutableFloat aFloat = new AMutableFloat(0);
        private final AMutableInt64 aInt64 = new AMutableInt64(0);
        private final AMutableInt32 aInt32 = new AMutableInt32(0);
        private final AMutableInt16 aInt16 = new AMutableInt16((short) 0);
        private final AMutableInt8 aInt8 = new AMutableInt8((byte) 0);
        @SuppressWarnings("rawtypes")
        private ISerializerDeserializer serde;

        NumericTruncEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx) throws HyracksDataException {
            eval = args[0].createScalarEvaluator(ctx);
            precision = args[1].createScalarEvaluator(ctx);
        }

        private int getPrecision() throws HyracksDataException {
            byte[] bytes = argPrecision.getByteArray();
            int offset = argPrecision.getStartOffset();

            if (bytes[offset] == ATypeTag.SERIALIZED_INT8_TYPE_TAG) {
                return AInt8SerializerDeserializer.getByte(bytes, offset + 1);
            } else if (bytes[offset] == ATypeTag.SERIALIZED_INT16_TYPE_TAG) {
                return AInt16SerializerDeserializer.getShort(bytes, offset + 1);
            } else if (bytes[offset] == ATypeTag.SERIALIZED_INT32_TYPE_TAG) {
                return AInt32SerializerDeserializer.getInt(bytes, offset + 1);
            } else if (bytes[offset] == ATypeTag.SERIALIZED_INT64_TYPE_TAG) {
                return (int) AInt64SerializerDeserializer.getLong(bytes, offset + 1);
            } else {
                throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, bytes[offset],
                        ATypeTag.SERIALIZED_INT8_TYPE_TAG, ATypeTag.SERIALIZED_INT16_TYPE_TAG,
                        ATypeTag.SERIALIZED_INT32_TYPE_TAG, ATypeTag.SERIALIZED_INT64_TYPE_TAG);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            resultStorage.reset();
            eval.evaluate(tuple, argValue);
            precision.evaluate(tuple, argPrecision);

            if (PointableHelper.checkAndSetMissingOrNull(result, argValue, argPrecision)) {
                return;
            }

            byte[] data = argValue.getByteArray();
            int offset = argValue.getStartOffset();

            if (data[offset] == ATypeTag.SERIALIZED_INT8_TYPE_TAG) {
                serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT8);
                byte val = AInt8SerializerDeserializer.getByte(data, offset + 1);
                aInt8.setValue(val);
                serde.serialize(aInt8, out);
            } else if (data[offset] == ATypeTag.SERIALIZED_INT16_TYPE_TAG) {
                serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT16);
                short val = AInt16SerializerDeserializer.getShort(data, offset + 1);
                aInt16.setValue(val);
                serde.serialize(aInt16, out);
            } else if (data[offset] == ATypeTag.SERIALIZED_INT32_TYPE_TAG) {
                serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
                int val = AInt32SerializerDeserializer.getInt(data, offset + 1);
                aInt32.setValue(val);
                serde.serialize(aInt32, out);
            } else if (data[offset] == ATypeTag.SERIALIZED_INT64_TYPE_TAG) {
                serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
                long val = AInt64SerializerDeserializer.getLong(data, offset + 1);
                aInt64.setValue(val);
                serde.serialize(aInt64, out);
            } else if (data[offset] == ATypeTag.SERIALIZED_FLOAT_TYPE_TAG) {
                serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AFLOAT);
                float val = AFloatSerializerDeserializer.getFloat(data, offset + 1);
                if (Float.isNaN(val) || Float.isInfinite(val) || Float.compare(val, -0.0F) == 0
                        || Float.compare(val, 0.0F) == 0) {
                    aFloat.setValue(val);
                    serde.serialize(aFloat, out);
                } else {
                    BigDecimal r = new BigDecimal(Float.toString(val));
                    aFloat.setValue(r.setScale(getPrecision(), BigDecimal.ROUND_DOWN).floatValue());
                    serde.serialize(aFloat, out);
                }
            } else if (data[offset] == ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG) {
                serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
                double val = ADoubleSerializerDeserializer.getDouble(data, offset + 1);
                if (Double.isNaN(val) || Double.isInfinite(val) || Double.compare(val, -0.0D) == 0
                        || Double.compare(val, 0.0D) == 0) {
                    aDouble.setValue(val);
                    serde.serialize(aDouble, out);
                } else {
                    BigDecimal r = new BigDecimal(Double.toString(val));
                    aDouble.setValue(r.setScale(getPrecision(), BigDecimal.ROUND_DOWN).doubleValue());
                    serde.serialize(aDouble, out);
                }
            } else {
                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, data[offset],
                        ATypeTag.SERIALIZED_INT8_TYPE_TAG, ATypeTag.SERIALIZED_INT16_TYPE_TAG,
                        ATypeTag.SERIALIZED_INT32_TYPE_TAG, ATypeTag.SERIALIZED_INT64_TYPE_TAG,
                        ATypeTag.SERIALIZED_FLOAT_TYPE_TAG, ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
            }
            result.set(resultStorage);
        }
    }

}
