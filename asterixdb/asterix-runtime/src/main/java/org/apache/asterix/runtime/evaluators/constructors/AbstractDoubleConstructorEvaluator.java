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

package org.apache.asterix.runtime.evaluators.constructors;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringUtil;

public abstract class AbstractDoubleConstructorEvaluator implements IScalarEvaluator {
    @SuppressWarnings("unchecked")
    protected static final ISerializerDeserializer<ADouble> DOUBLE_SERDE =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

    protected static final IBinaryComparator UTF8_BINARY_CMP =
            BinaryComparatorFactoryProvider.UTF8STRING_POINTABLE_INSTANCE.createBinaryComparator();

    protected static final byte[] POSITIVE_INF = UTF8StringUtil.writeStringToBytes("INF");
    protected static final byte[] NEGATIVE_INF = UTF8StringUtil.writeStringToBytes("-INF");
    protected static final byte[] NAN = UTF8StringUtil.writeStringToBytes("NaN");

    protected final IScalarEvaluator inputEval;
    protected final ArrayBackedValueStorage resultStorage;
    protected final DataOutput out;
    protected final IPointable inputArg;
    protected final AMutableDouble aDouble;
    protected final UTF8StringPointable utf8Ptr;

    protected AbstractDoubleConstructorEvaluator(IScalarEvaluator inputEval) {
        this.inputEval = inputEval;
        resultStorage = new ArrayBackedValueStorage();
        out = resultStorage.getDataOutput();
        inputArg = new VoidPointable();
        aDouble = new AMutableDouble(0);
        utf8Ptr = new UTF8StringPointable();
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        try {
            inputEval.evaluate(tuple, inputArg);
            resultStorage.reset();
            evaluateImpl(result);
        } catch (IOException e) {
            throw new InvalidDataFormatException(getIdentifier(), e, ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
        }
    }

    protected void evaluateImpl(IPointable result) throws IOException {
        byte[] bytes = inputArg.getByteArray();
        int offset = inputArg.getStartOffset();
        byte tt = bytes[offset];
        if (tt == ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG) {
            result.set(inputArg);
        } else if (tt == ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            int len = inputArg.getLength();
            if (UTF8_BINARY_CMP.compare(bytes, offset + 1, len - 1, POSITIVE_INF, 0, 5) == 0) {
                setDouble(result, Double.POSITIVE_INFINITY);
            } else if (UTF8_BINARY_CMP.compare(bytes, offset + 1, len - 1, NEGATIVE_INF, 0, 6) == 0) {
                setDouble(result, Double.NEGATIVE_INFINITY);
            } else if (UTF8_BINARY_CMP.compare(bytes, offset + 1, len - 1, NAN, 0, 5) == 0) {
                setDouble(result, Double.NaN);
            } else {
                utf8Ptr.set(bytes, offset + 1, len - 1);
                try {
                    setDouble(result, Double.parseDouble(utf8Ptr.toString()));
                } catch (NumberFormatException e) {
                    handleUparseableString(result, e);
                }
            }
        } else {
            throw new TypeMismatchException(getIdentifier(), 0, tt, ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        }
    }

    protected void handleUparseableString(IPointable result, NumberFormatException e) throws HyracksDataException {
        throw new InvalidDataFormatException(getIdentifier(), e, ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
    }

    protected void setDouble(IPointable result, double value) throws HyracksDataException {
        aDouble.setValue(value);
        DOUBLE_SERDE.serialize(aDouble, out);
        result.set(resultStorage);
    }

    protected abstract FunctionIdentifier getIdentifier();
}
