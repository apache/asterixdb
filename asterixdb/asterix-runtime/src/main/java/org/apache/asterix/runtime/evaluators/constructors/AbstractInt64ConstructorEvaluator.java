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

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.common.NumberUtils;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractInt64ConstructorEvaluator implements IScalarEvaluator {
    @SuppressWarnings("unchecked")
    protected static final ISerializerDeserializer<AInt64> INT64_SERDE =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);

    protected final IScalarEvaluator inputEval;
    protected final SourceLocation sourceLoc;
    protected final IPointable inputArg;
    protected final ArrayBackedValueStorage resultStorage;
    protected final DataOutput out;
    protected final AMutableInt64 aInt64;
    protected final UTF8StringPointable utf8Ptr;

    protected AbstractInt64ConstructorEvaluator(IScalarEvaluator inputEval, SourceLocation sourceLoc) {
        this.inputEval = inputEval;
        this.sourceLoc = sourceLoc;
        inputArg = new VoidPointable();
        resultStorage = new ArrayBackedValueStorage();
        out = resultStorage.getDataOutput();
        aInt64 = new AMutableInt64(0);
        utf8Ptr = new UTF8StringPointable();
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
            throw new InvalidDataFormatException(sourceLoc, getIdentifier(), e, ATypeTag.SERIALIZED_INT64_TYPE_TAG);
        }
    }

    protected void evaluateImpl(IPointable result) throws IOException {
        byte[] bytes = inputArg.getByteArray();
        int startOffset = inputArg.getStartOffset();

        byte tt = bytes[startOffset];
        if (tt == ATypeTag.SERIALIZED_INT64_TYPE_TAG) {
            result.set(inputArg);
        } else if (tt == ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            utf8Ptr.set(bytes, startOffset + 1, inputArg.getLength() - 1);
            if (NumberUtils.parseInt64(utf8Ptr, aInt64)) {
                INT64_SERDE.serialize(aInt64, out);
                result.set(resultStorage);
            } else {
                handleUnparseableString(result);
            }
        } else {
            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, tt, ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        }
    }

    protected void handleUnparseableString(IPointable result) throws HyracksDataException {
        throw new InvalidDataFormatException(sourceLoc, getIdentifier(), ATypeTag.SERIALIZED_INT64_TYPE_TAG);
    }

    protected abstract FunctionIdentifier getIdentifier();
}
