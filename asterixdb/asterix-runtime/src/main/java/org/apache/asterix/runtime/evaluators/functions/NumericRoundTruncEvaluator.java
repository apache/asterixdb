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

package org.apache.asterix.runtime.evaluators.functions;

import static org.apache.asterix.om.types.ATypeTag.VALUE_TYPE_MAPPING;

import java.io.DataOutput;
import java.math.BigDecimal;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableFloat;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.common.ArgumentUtils;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Used by {@link NumericTruncDescriptor trunc()} and {@link NumericRoundHalfToEven2Descriptor round-half-to-even()}.
 */
class NumericRoundTruncEvaluator extends AbstractScalarEval {

    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput out = resultStorage.getDataOutput();
    private final IPointable valueArg = new VoidPointable();
    private final IPointable precisionArg = new VoidPointable();
    private final AMutableDouble aDouble = new AMutableDouble(0);
    private final AMutableFloat aFloat = new AMutableFloat(0);
    private final AMutableInt32 aInt32 = new AMutableInt32(0);
    private final IEvaluatorContext ctx;
    private final IScalarEvaluator valueEval;
    private final IScalarEvaluator precisionEval;
    private final int roundingMode;

    NumericRoundTruncEvaluator(IEvaluatorContext ctx, IScalarEvaluatorFactory[] args, int roundingMode,
            FunctionIdentifier funID, SourceLocation srcLoc) throws HyracksDataException {
        super(srcLoc, funID);
        this.ctx = ctx;
        this.valueEval = args[0].createScalarEvaluator(ctx);
        this.precisionEval = args[1].createScalarEvaluator(ctx);
        this.roundingMode = roundingMode;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        valueEval.evaluate(tuple, valueArg);
        precisionEval.evaluate(tuple, precisionArg);

        if (PointableHelper.checkAndSetMissingOrNull(result, valueArg, precisionArg)) {
            return;
        }

        byte[] value = valueArg.getByteArray();
        int offset = valueArg.getStartOffset();
        byte valueTag = value[offset];
        if (ATypeHierarchy.getTypeDomain(VALUE_TYPE_MAPPING[valueTag]) != ATypeHierarchy.Domain.NUMERIC) {
            ExceptionUtil.warnTypeMismatch(ctx, srcLoc, funID, valueTag, 0, ArgumentUtils.NUMERIC_TYPES);
            PointableHelper.setNull(result);
            return;
        }
        if (!ArgumentUtils.setInteger(ctx, srcLoc, funID, 1, precisionArg.getByteArray(), precisionArg.getStartOffset(),
                aInt32)) {
            PointableHelper.setNull(result);
            return;
        }

        if (valueTag == ATypeTag.SERIALIZED_INT8_TYPE_TAG || valueTag == ATypeTag.SERIALIZED_INT16_TYPE_TAG
                || valueTag == ATypeTag.SERIALIZED_INT32_TYPE_TAG || valueTag == ATypeTag.SERIALIZED_INT64_TYPE_TAG) {
            result.set(valueArg);
            return;
        }

        @SuppressWarnings("rawtypes")
        ISerializerDeserializer serde;
        if (valueTag == ATypeTag.SERIALIZED_FLOAT_TYPE_TAG) {
            serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AFLOAT);
            float val = AFloatSerializerDeserializer.getFloat(value, offset + 1);
            if (Float.isNaN(val) || Float.isInfinite(val) || Float.compare(val, -0.0F) == 0
                    || Float.compare(val, 0.0F) == 0) {
                aFloat.setValue(val);
                serde.serialize(aFloat, out);
            } else {
                BigDecimal r = new BigDecimal(Float.toString(val));
                aFloat.setValue(r.setScale(aInt32.getIntegerValue(), roundingMode).floatValue());
                serde.serialize(aFloat, out);
            }
        } else if (valueTag == ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG) {
            serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
            double val = ADoubleSerializerDeserializer.getDouble(value, offset + 1);
            if (Double.isNaN(val) || Double.isInfinite(val) || Double.compare(val, -0.0D) == 0
                    || Double.compare(val, 0.0D) == 0) {
                aDouble.setValue(val);
                serde.serialize(aDouble, out);
            } else {
                BigDecimal r = new BigDecimal(Double.toString(val));
                aDouble.setValue(r.setScale(aInt32.getIntegerValue(), roundingMode).doubleValue());
                serde.serialize(aDouble, out);
            }
        } else {
            ExceptionUtil.warnUnsupportedType(ctx, srcLoc, funID.getName(), VALUE_TYPE_MAPPING[valueTag]);
            PointableHelper.setNull(result);
            return;
        }
        result.set(resultStorage);
    }
}
