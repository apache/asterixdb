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

import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
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
 * This function receives three arguments, first argument representing the value, and second argument representing
 * the number of times to apply the bitwise operation, and third argument (optional) is a boolean flag. This evaluator
 * is used by the bitwise SHIFT functions.
 * These functions can be applied only to int64 type numeric values.
 *
 * The function has the following behavior:
 * For the first and second arguments:
 * - If the argument is missing or null, then missing or null is returned, respectively.
 * - If the argument is int64, then int64 is returned.
 * - If the argument is float or double, then int64 or null is returned, depending on the argument value.
 * - If the argument is any other type, then null is returned.
 *
 * For the third argument:
 * - If the argument is missing or null, then missing or null is returned, respectively.
 * - If the argument is boolean, then int64 is returned.
 * - If the argument is any other type, then null is returned.
 */

class BitValueCountFlagEvaluator extends AbstractScalarEval {

    // Result members
    private final AMutableInt64 resultMutableInt64 = new AMutableInt64(0);
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();

    // Evaluators and Pointables
    private final IScalarEvaluator valueEvaluator;
    private final IScalarEvaluator countEvaluator;
    private IScalarEvaluator flagEvaluator;
    private final IPointable valuePointable = new VoidPointable();
    private final IPointable countPointable = new VoidPointable();
    private IPointable flagPointable;

    // Serializer/Deserializer
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer aInt64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);

    private final IEvaluatorContext context;

    BitValueCountFlagEvaluator(IEvaluatorContext context, IScalarEvaluatorFactory[] argEvaluatorFactories,
            FunctionIdentifier functionIdentifier, SourceLocation sourceLocation) throws HyracksDataException {
        super(sourceLocation, functionIdentifier);

        this.context = context;

        // Evaluator
        valueEvaluator = argEvaluatorFactories[0].createScalarEvaluator(context);
        countEvaluator = argEvaluatorFactories[1].createScalarEvaluator(context);
        if (argEvaluatorFactories.length > 2) {
            flagEvaluator = argEvaluatorFactories[2].createScalarEvaluator(context);
            flagPointable = new VoidPointable();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        valueEvaluator.evaluate(tuple, valuePointable);
        countEvaluator.evaluate(tuple, countPointable);

        if (flagEvaluator != null) {
            flagEvaluator.evaluate(tuple, flagPointable);
        }

        if (PointableHelper.checkAndSetMissingOrNull(result, valuePointable, countPointable, flagPointable)) {
            return;
        }

        // First argument
        byte[] valueBytes = valuePointable.getByteArray();
        int valueStartOffset = valuePointable.getStartOffset();

        // Type and value validity check
        if (!PointableHelper.isValidLongValue(valueBytes, valueStartOffset, true)) {
            ExceptionUtil.warnTypeMismatch(context, sourceLoc, functionIdentifier, valueBytes[valueStartOffset], 0,
                    ATypeTag.BIGINT);
            PointableHelper.setNull(result);
            return;
        }

        // Second argument
        byte[] countBytes = countPointable.getByteArray();
        int countStartOffset = countPointable.getStartOffset();

        // Type and Value validity check
        if (!PointableHelper.isValidLongValue(countBytes, countStartOffset, true)) {
            ExceptionUtil.warnTypeMismatch(context, sourceLoc, functionIdentifier, countBytes[countStartOffset], 1,
                    ATypeTag.BIGINT);
            PointableHelper.setNull(result);
            return;
        }

        // Third argument
        boolean isRotate = false;
        if (flagEvaluator != null) {
            byte[] flagBytes = flagPointable.getByteArray();
            int flagStartOffset = flagPointable.getStartOffset();
            ATypeTag flagTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(flagBytes[flagStartOffset]);

            if (flagTypeTag != ATypeTag.BOOLEAN) {
                ExceptionUtil.warnTypeMismatch(context, sourceLoc, functionIdentifier, flagBytes[flagStartOffset], 2,
                        ATypeTag.BOOLEAN);
                PointableHelper.setNull(result);
                return;
            }

            isRotate = ABooleanSerializerDeserializer.getBoolean(flagBytes, flagStartOffset + 1);
        }

        // Result holder and count
        long longValue = ATypeHierarchy.getLongValue(functionIdentifier.getName(), 0, valueBytes, valueStartOffset);
        long count = ATypeHierarchy.getLongValue(functionIdentifier.getName(), 1, countBytes, countStartOffset);

        // Positive value is left shifting, negative value is right shifting, rotate if needed, do nothing on 0 count
        // Note, when rotating, for each 64 bits, the rotation is repeated, so rotating by 1 is same as rotating by 65,
        // rotating by 2 is same as rotating by 66, so we can make the rotation count % 64
        if (count > 0) {
            if (isRotate) {
                longValue = Long.rotateLeft(longValue, (int) (count % 64));
            } else {
                longValue = longValue << count;
            }
        }

        if (count < 0) {
            if (isRotate) {
                longValue = Long.rotateRight(longValue, (int) Math.abs(count % -64));
            } else {
                longValue = longValue >> Math.abs(count);
            }
        }

        resultStorage.reset();
        resultMutableInt64.setValue(longValue);
        aInt64Serde.serialize(resultMutableInt64, resultStorage.getDataOutput());
        result.set(resultStorage);
    }
}
