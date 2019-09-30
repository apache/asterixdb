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

package org.apache.asterix.runtime.evaluators.functions.bitwise;

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.functions.AbstractScalarEval;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
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
 * This function receives one or more arguments, applies the bitwise operation to all of the arguments
 * and returns the result. This evaluator is used by the Bitwise AND, OR, XOR, ... etc functions.
 * These functions can be applied only to int64 type numeric values.
 *
 * The function has the following behavior:
 * - If the argument is missing or null, then missing or null is returned, respectively.
 * - If the argument is int64, then int64 is returned.
 * - If the argument is float or double, then int64 or null is returned, depending on the argument value.
 * - If the argument is any other type, then null is returned.
 */

abstract class AbstractBitMultipleValuesEvaluator extends AbstractScalarEval {

    // Result members
    private final AMutableInt64 resultMutableInt64 = new AMutableInt64(0);
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();

    // Evaluators and Pointables
    private final IScalarEvaluator[] argEvaluators;
    private final IPointable[] argPointables;

    // Serializer/Deserializer
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer aInt64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);

    private final IEvaluatorContext context;

    AbstractBitMultipleValuesEvaluator(IEvaluatorContext context, IScalarEvaluatorFactory[] argEvaluatorFactories,
            FunctionIdentifier functionIdentifier, SourceLocation sourceLocation) throws HyracksDataException {
        super(sourceLocation, functionIdentifier);

        this.context = context;

        // Evaluators and Pointables
        argPointables = new IPointable[argEvaluatorFactories.length];
        argEvaluators = new IScalarEvaluator[argEvaluatorFactories.length];
        for (int i = 0; i < argEvaluatorFactories.length; i++) {
            argEvaluators[i] = argEvaluatorFactories[i].createScalarEvaluator(context);
            argPointables[i] = new VoidPointable();
        }
    }

    // Abstract methods
    abstract long applyBitwiseOperation(long value1, long value2);

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        boolean isReturnNull = false;

        // Evaluate and check the arguments (Missing/Null checks)
        for (int i = 0; i < argEvaluators.length; i++) {
            argEvaluators[i].evaluate(tuple, argPointables[i]);

            if (PointableHelper.checkAndSetMissingOrNull(result, argPointables[i])) {
                if (result.getByteArray()[result.getStartOffset()] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
                    return;
                }

                // null value, but check other arguments for missing first (higher priority)
                isReturnNull = true;
            }
        }

        if (isReturnNull) {
            PointableHelper.setNull(result);
            return;
        }

        // First argument
        byte[] bytes = argPointables[0].getByteArray();
        int startOffset = argPointables[0].getStartOffset();

        // Type and value validity check
        if (!PointableHelper.isValidLongValue(bytes, startOffset, true)) {
            ExceptionUtil.warnTypeMismatch(context, sourceLoc, functionIdentifier, bytes[startOffset], 0,
                    ATypeTag.BIGINT);
            PointableHelper.setNull(result);
            return;
        }

        // Result holder
        long longResult = ATypeHierarchy.getLongValue(functionIdentifier.getName(), 0, bytes, startOffset);

        // Loop and do the bitwise operation for all the arguments (start from index 1, we did first argument above)
        for (int i = 1; i < argPointables.length; i++) {
            bytes = argPointables[i].getByteArray();
            startOffset = argPointables[i].getStartOffset();

            // Type and value validity check
            if (!PointableHelper.isValidLongValue(bytes, startOffset, true)) {
                ExceptionUtil.warnTypeMismatch(context, sourceLoc, functionIdentifier, bytes[startOffset], i,
                        ATypeTag.BIGINT);
                PointableHelper.setNull(result);
                return;
            }

            long nextValue = ATypeHierarchy.getLongValue(functionIdentifier.getName(), i, bytes, startOffset);
            longResult = applyBitwiseOperation(longResult, nextValue);
        }

        // Write the result
        resultStorage.reset();
        resultMutableInt64.setValue(longResult);
        aInt64Serde.serialize(resultMutableInt64, resultStorage.getDataOutput());
        result.set(resultStorage);
    }
}
