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
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringUtil;

public abstract class AbstractBooleanConstructorEvaluator implements IScalarEvaluator {
    @SuppressWarnings("unchecked")
    protected static final ISerializerDeserializer<ABoolean> BOOLEAN_SERDE =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);

    protected static final IBinaryComparator UTF8_BINARY_CMP =
            BinaryComparatorFactoryProvider.UTF8STRING_POINTABLE_INSTANCE.createBinaryComparator();

    protected static final byte[] TRUE = UTF8StringUtil.writeStringToBytes("true");
    protected static final byte[] FALSE = UTF8StringUtil.writeStringToBytes("false");

    protected final IScalarEvaluator inputEval;
    protected final SourceLocation sourceLoc;
    protected final IPointable inputArg;
    protected final ArrayBackedValueStorage resultStorage;
    protected final DataOutput out;

    protected AbstractBooleanConstructorEvaluator(IScalarEvaluator inputEval, SourceLocation sourceLoc) {
        this.inputEval = inputEval;
        this.sourceLoc = sourceLoc;
        inputArg = new VoidPointable();
        resultStorage = new ArrayBackedValueStorage();
        out = resultStorage.getDataOutput();
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        try {
            inputEval.evaluate(tuple, inputArg);
            resultStorage.reset();

            if (PointableHelper.checkAndSetMissingOrNull(result, inputArg)) {
                return;
            }

            evaluateImpl(result);
        } catch (IOException e) {
            throw new InvalidDataFormatException(sourceLoc, getIdentifier(), e, ATypeTag.SERIALIZED_BOOLEAN_TYPE_TAG);
        }
    }

    protected void evaluateImpl(IPointable result) throws HyracksDataException {
        byte[] bytes = inputArg.getByteArray();
        int startOffset = inputArg.getStartOffset();
        byte tt = bytes[startOffset];
        if (tt == ATypeTag.SERIALIZED_BOOLEAN_TYPE_TAG) {
            result.set(inputArg);
        } else if (tt == ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            int len = inputArg.getLength();
            if (UTF8_BINARY_CMP.compare(bytes, startOffset + 1, len - 1, TRUE, 0, TRUE.length) == 0) {
                setBoolean(result, true);
            } else if (UTF8_BINARY_CMP.compare(bytes, startOffset + 1, len - 1, FALSE, 0, FALSE.length) == 0) {
                setBoolean(result, false);
            } else {
                throw new InvalidDataFormatException(sourceLoc, getIdentifier(), ATypeTag.SERIALIZED_BOOLEAN_TYPE_TAG);
            }
        } else {
            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, tt, ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        }
    }

    protected void setBoolean(IPointable result, boolean value) throws HyracksDataException {
        BOOLEAN_SERDE.serialize(ABoolean.valueOf(value), out);
        result.set(resultStorage);
    }

    protected abstract FunctionIdentifier getIdentifier();
}
