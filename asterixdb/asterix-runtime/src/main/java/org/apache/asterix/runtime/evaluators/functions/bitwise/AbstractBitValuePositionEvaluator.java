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

import java.io.IOException;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.WarningUtil;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.AbstractScalarEval;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * This function receives two arguments, first argument representing the value, and second argument representing
 * the position to apply the operation at. The position can be a single value or an array of values. This evaluator
 * is used by the bitwise CLEAR and SET functions.
 *
 * These functions can be applied only to int64 type numeric values.
 *
 * The function has the following behavior:
 * For the first argument:
 * - If the argument is missing or null, then missing or null is returned, respectively.
 * - If the argument is int64, then int64 is returned.
 * - If the argument is float or double, then int64 or null is returned, depending on the argument value.
 * - If the argument is any other type, then null is returned.
 *
 * For the second argument:
 * - If the argument is missing or null, then missing or null is returned, respectively.
 * - If the argument is int64, then int64 is returned.
 * - If the argument is float or double, then int64 or null is returned, depending on the argument value.
 * - If the argument is an array:
 *      - If the array item type is int64, then int64 or boolean is returned.
 *      - If the array item type is float or double, then int64 or null is returned, depending on the argument.
 *      - If the array item type is any other type, then null is returned.
 * - If the argument is any other type, then null is returned.
 */

abstract class AbstractBitValuePositionEvaluator extends AbstractScalarEval {

    // Result members
    private long longResult;
    private final AMutableInt64 resultMutableInt64 = new AMutableInt64(0);
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();

    // Evaluators and Pointables
    private final IScalarEvaluator valueEvaluator;
    private final IScalarEvaluator positionEvaluator;
    private final IPointable valuePointable = new VoidPointable();
    private final IPointable positionPointable = new VoidPointable();

    // List accessor
    private final ListAccessor listAccessor = new ListAccessor();
    private final IPointable listItemPointable = new VoidPointable();

    // Serializer/Deserializer
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer aInt64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);

    private final IEvaluatorContext context;
    private static final byte[] secondArgumentExpectedTypes =
            new byte[] { ATypeTag.SERIALIZED_INT64_TYPE_TAG, ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG };

    AbstractBitValuePositionEvaluator(IEvaluatorContext context, IScalarEvaluatorFactory[] argEvaluatorFactories,
            FunctionIdentifier functionIdentifier, SourceLocation sourceLocation) throws HyracksDataException {
        super(sourceLocation, functionIdentifier);
        this.context = context;

        // Evaluators
        valueEvaluator = argEvaluatorFactories[0].createScalarEvaluator(context);
        positionEvaluator = argEvaluatorFactories[1].createScalarEvaluator(context);
    }

    // Abstract methods
    abstract long applyBitwiseOperation(long value, long position);

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        valueEvaluator.evaluate(tuple, valuePointable);
        positionEvaluator.evaluate(tuple, positionPointable);

        if (PointableHelper.checkAndSetMissingOrNull(result, valuePointable, positionPointable)) {
            return;
        }

        // Value argument
        byte[] valueBytes = valuePointable.getByteArray();
        int valueStartOffset = valuePointable.getStartOffset();

        // Type and value validity check
        if (!PointableHelper.isValidLongValue(valueBytes, valueStartOffset, true)) {
            ExceptionUtil.warnTypeMismatch(context, sourceLoc, functionIdentifier, valueBytes[valueStartOffset], 0,
                    ATypeTag.BIGINT);
            PointableHelper.setNull(result);
            return;
        }

        // Position argument
        byte[] positionBytes = positionPointable.getByteArray();
        int positionStartOffset = positionPointable.getStartOffset();
        ATypeTag positionTypeTag =
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(positionBytes[positionStartOffset]);

        // Type validity check (for position argument, array is a valid type as well)
        if (!ATypeHierarchy.canPromote(positionTypeTag, ATypeTag.DOUBLE) && positionTypeTag != ATypeTag.ARRAY) {
            ExceptionUtil.warnTypeMismatch(context, sourceLoc, functionIdentifier, positionBytes[positionStartOffset],
                    1, secondArgumentExpectedTypes);
            PointableHelper.setNull(result);
            return;
        }

        // Result long value
        longResult = ATypeHierarchy.getLongValue(functionIdentifier.getName(), 0, valueBytes, valueStartOffset);

        // If any operation returns false, the result should be null
        boolean isSuccessfulOperation;

        // From here onward, we got valid arguments, so let's handle numeric or array position argument
        if (positionTypeTag != ATypeTag.ARRAY) {
            isSuccessfulOperation = applyBitWiseOperationWithNumericAsPosition(positionBytes, positionStartOffset);
        } else {
            isSuccessfulOperation = applyBitWiseOperationWithArrayAsPosition(positionBytes, positionStartOffset);
        }

        if (!isSuccessfulOperation) {
            PointableHelper.setNull(result);
            return;
        }

        // Write the result
        resultStorage.reset();
        resultMutableInt64.setValue(longResult);
        aInt64Serde.serialize(resultMutableInt64, resultStorage.getDataOutput());
        result.set(resultStorage);
    }

    /**
     * This method applies the bitwise operation on the value at the specified positions in the array.
     *
     * @param bytes bytes of the position
     * @param startOffset start offset of the position
     * @return returns {@code true} if all operations are successful, {@code false} otherwise.
     *
     * @throws HyracksDataException HyracksDataException
     */
    private boolean applyBitWiseOperationWithArrayAsPosition(byte[] bytes, int startOffset)
            throws HyracksDataException {

        listAccessor.reset(bytes, startOffset);

        // In case of an empty array, we return the value as is without change
        if (listAccessor.size() == 0) {
            return true;
        }

        try {
            // For each item in the array, apply the bitwise operation at that position
            for (int i = 0; i < listAccessor.size(); i++) {
                resultStorage.reset();
                listAccessor.getOrWriteItem(i, listItemPointable, resultStorage);

                byte[] itemBytes = listItemPointable.getByteArray();
                int itemStartOffset = listItemPointable.getStartOffset();

                if (!applyBitWiseOperationWithNumericAsPosition(itemBytes, itemStartOffset)) {
                    return false;
                }
            }

            return true;
        } catch (IOException ex) {
            throw HyracksDataException.create(ex);
        }
    }

    /**
     * This method applies the bitwise operation on the value at the specified position.
     *
     * @param bytes data bytes
     * @param startOffset start offset
     * @return returns {@code true} if all operations are successful, {@code false} otherwise.
     */
    private boolean applyBitWiseOperationWithNumericAsPosition(byte[] bytes, int startOffset)
            throws HyracksDataException {

        // Value validity check
        if (!PointableHelper.isValidLongValue(bytes, startOffset, true)) {
            ExceptionUtil.warnTypeMismatch(context, sourceLoc, functionIdentifier, bytes[startOffset], 1,
                    ATypeTag.BIGINT);
            return false;
        }

        long position = ATypeHierarchy.getLongValue(functionIdentifier.getName(), 1, bytes, startOffset);

        // Ensure the position is between 1 and 64 (int64 has 64 bits)
        if (position < 1 || position > 64) {
            handleOutOfRangeInput(1, 1, 64, position);
            return false;
        }

        longResult = applyBitwiseOperation(longResult, position);

        return true;
    }

    private void handleOutOfRangeInput(int inputPosition, int startLimit, int endLimit, long actual) {
        IWarningCollector warningCollector = context.getWarningCollector();
        if (warningCollector.shouldWarn()) {
            warningCollector.warn(WarningUtil.forAsterix(sourceLoc, ErrorCode.VALUE_OUT_OF_RANGE, functionIdentifier,
                    ExceptionUtil.indexToPosition(inputPosition), startLimit, endLimit, actual));
        }
    }
}
