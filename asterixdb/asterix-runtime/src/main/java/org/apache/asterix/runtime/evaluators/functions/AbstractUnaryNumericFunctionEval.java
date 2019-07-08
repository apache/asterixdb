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

import java.io.DataOutput;
import java.io.IOException;

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
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
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
 * An abstract class for functions that take a numeric value as input and output a numeric value.
 */
abstract class AbstractUnaryNumericFunctionEval implements IScalarEvaluator {

    // For the argument.
    protected final IPointable argPtr = new VoidPointable();

    // For the result.
    protected final AMutableDouble aDouble = new AMutableDouble(0);
    protected final AMutableFloat aFloat = new AMutableFloat(0);
    protected final AMutableInt64 aInt64 = new AMutableInt64(0);
    protected final AMutableInt32 aInt32 = new AMutableInt32(0);
    protected final AMutableInt16 aInt16 = new AMutableInt16((short) 0);
    protected final AMutableInt8 aInt8 = new AMutableInt8((byte) 0);
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput dataOutput = resultStorage.getDataOutput();

    // The evaluator for the argument expression.
    private final IScalarEvaluator argEval;

    @SuppressWarnings("rawtypes")
    protected ISerializerDeserializer int8Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT8);
    @SuppressWarnings("rawtypes")
    protected ISerializerDeserializer int16Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT16);
    @SuppressWarnings("rawtypes")
    protected ISerializerDeserializer int32Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
    @SuppressWarnings("rawtypes")
    protected ISerializerDeserializer int64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    @SuppressWarnings("rawtypes")
    protected ISerializerDeserializer floatSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AFLOAT);
    @SuppressWarnings("rawtypes")
    protected ISerializerDeserializer doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

    // The function identifier, used for error messages.
    private final FunctionIdentifier funcID;
    private final SourceLocation sourceLoc;

    public AbstractUnaryNumericFunctionEval(IEvaluatorContext context, IScalarEvaluatorFactory argEvalFactory,
            FunctionIdentifier funcID, SourceLocation sourceLoc) throws HyracksDataException {
        this.argEval = argEvalFactory.createScalarEvaluator(context);
        this.funcID = funcID;
        this.sourceLoc = sourceLoc;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        argEval.evaluate(tuple, argPtr);

        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr)) {
            return;
        }

        byte[] data = argPtr.getByteArray();
        int offset = argPtr.getStartOffset();

        if (data[offset] == ATypeTag.SERIALIZED_INT8_TYPE_TAG) {
            byte val = AInt8SerializerDeserializer.getByte(data, offset + 1);
            processInt8(val, result);
        } else if (data[offset] == ATypeTag.SERIALIZED_INT16_TYPE_TAG) {
            short val = AInt16SerializerDeserializer.getShort(data, offset + 1);
            processInt16(val, result);
        } else if (data[offset] == ATypeTag.SERIALIZED_INT32_TYPE_TAG) {
            int val = AInt32SerializerDeserializer.getInt(data, offset + 1);
            processInt32(val, result);
        } else if (data[offset] == ATypeTag.SERIALIZED_INT64_TYPE_TAG) {
            long val = AInt64SerializerDeserializer.getLong(data, offset + 1);
            processInt64(val, result);
        } else if (data[offset] == ATypeTag.SERIALIZED_FLOAT_TYPE_TAG) {
            float val = AFloatSerializerDeserializer.getFloat(data, offset + 1);
            processFloat(val, result);
        } else if (data[offset] == ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG) {
            double val = ADoubleSerializerDeserializer.getDouble(data, offset + 1);
            processDouble(val, result);
        } else {
            throw new TypeMismatchException(sourceLoc, funcID, 0, data[offset], ATypeTag.SERIALIZED_INT8_TYPE_TAG,
                    ATypeTag.SERIALIZED_INT16_TYPE_TAG, ATypeTag.SERIALIZED_INT32_TYPE_TAG,
                    ATypeTag.SERIALIZED_INT64_TYPE_TAG, ATypeTag.SERIALIZED_FLOAT_TYPE_TAG,
                    ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
        }
    }

    /**
     * Processes an int8 argument.
     *
     * @param arg
     *            , the value of the argument.
     * @param resultPointable
     *            ,
     *            the pointable that should be set to the result location.
     */
    protected abstract void processInt8(byte arg, IPointable resultPointable) throws HyracksDataException;

    /**
     * Processes an int16 argument.
     *
     * @param arg
     *            , the value of the argument.
     * @param resultPointable
     *            ,
     *            the pointable that should be set to the result location.
     */
    protected abstract void processInt16(short arg, IPointable resultPointable) throws HyracksDataException;

    /**
     * Processes an int32 argument.
     *
     * @param arg
     *            , the value of the argument.
     * @param resultPointable
     *            ,
     *            the pointable that should be set to the result location.
     */
    protected abstract void processInt32(int arg, IPointable resultPointable) throws HyracksDataException;

    /**
     * Processes an int64 argument.
     *
     * @param arg
     *            , the value of the argument.
     * @param resultPointable
     *            ,
     *            the pointable that should be set to the result location.
     */
    protected abstract void processInt64(long arg, IPointable resultPointable) throws HyracksDataException;

    /**
     * Processes a float argument.
     *
     * @param arg
     *            , the value of the argument.
     * @param resultPointable
     *            ,
     *            the pointable that should be set to the result location.
     */
    protected abstract void processFloat(float arg, IPointable resultPointable) throws HyracksDataException;

    /**
     * Processes a double argument.
     *
     * @param arg
     *            , the value of the argument.
     * @param resultPointable
     *            ,
     *            the pointable that should be set to the result location.
     */
    protected abstract void processDouble(double arg, IPointable resultPointable) throws HyracksDataException;

    // Serializes result into the result storage.
    @SuppressWarnings("unchecked")
    protected void serialize(IAObject result, ISerializerDeserializer serde, IPointable resultPointable)
            throws HyracksDataException {
        try {
            serde.serialize(result, dataOutput);
            resultPointable.set(resultStorage);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
