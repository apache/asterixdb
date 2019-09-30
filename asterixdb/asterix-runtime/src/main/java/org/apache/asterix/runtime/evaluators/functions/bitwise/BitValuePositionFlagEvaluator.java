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
import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
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
 * This function receives two or three arguments, first argument representing the value, second argument representing
 * the position to apply the operation at, and third argument (optional) represents a boolean flag. The position can
 * be a single value or an array of values. This evaluator is used by the bitwise BITTEST and ISBITSET
 * functions.
 *
 * These functions can be applied only to int64 type numeric values.
 *
 * The function has the following behavior:
 * For the first argument:
 * - If the argument is missing or null, then missing or null is returned, respectively.
 * - If the argument is int64, then boolean is returned.
 * - If the argument is float or double, then boolean or null is returned, depending on the argument value.
 * - If the argument is any other type, then null is returned.
 *
 * For the second argument:
 * - If the argument is missing or null, then missing or null is returned, respectively.
 * - If the argument is int64, then boolean is returned.
 * - If the argument is float or double, then boolean or null is returned, depending on the argument value.
 * - If the argument is an array:
 *      - If the array item type is int64, then int64 or boolean is returned.
 *      - If the array item type is float or double, then boolean or null is returned, depending on the argument.
 *      - If the array item type  is any other type, then null is returned.
 * - If the argument is any other type, then null is returned.
 *
 * For the third argument:
 * - If the argument is missing or null, then missing or null is returned, respectively.
 * - If the argument is boolean, then boolean is returned.
 * - If the argument is any other type, then null is returned.
 */

class BitValuePositionFlagEvaluator extends AbstractScalarEval {

    // Result members
    private ABoolean resultBoolean;
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();

    // This flag has a special purpose. In some cases, when an array of positions is passed, checking
    // some arguments might be enough to return the final result, but, instead of stopping, we need to
    // continue looping because if any value in the positions array is invalid, we should return a null instead of
    // the boolean result, this flag will keep the loop going just for checking the values, while the
    // final result is already set previously.
    private boolean isStopUpdatingResultBoolean = false;

    // Evaluators and Pointables
    private final IScalarEvaluator valueEvaluator;
    private final IScalarEvaluator positionEvaluator;
    private IScalarEvaluator flagEvaluator;
    private final IPointable valuePointable = new VoidPointable();
    private final IPointable positionPointable = new VoidPointable();
    private IPointable flagPointable;

    // List accessor
    private final ListAccessor listAccessor = new ListAccessor();
    private final IPointable listItemPointable = new VoidPointable();

    // Serializer/Deserializer
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer aBooleanSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);

    private final IEvaluatorContext context;
    private static final byte[] secondArgumentExpectedTypes =
            new byte[] { ATypeTag.SERIALIZED_INT64_TYPE_TAG, ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG };

    BitValuePositionFlagEvaluator(IEvaluatorContext context, IScalarEvaluatorFactory[] argEvaluatorFactories,
            FunctionIdentifier functionIdentifier, SourceLocation sourceLocation) throws HyracksDataException {
        super(sourceLocation, functionIdentifier);

        this.context = context;

        // Evaluators
        valueEvaluator = argEvaluatorFactories[0].createScalarEvaluator(context);
        positionEvaluator = argEvaluatorFactories[1].createScalarEvaluator(context);

        if (argEvaluatorFactories.length > 2) {
            flagEvaluator = argEvaluatorFactories[2].createScalarEvaluator(context);
            flagPointable = new VoidPointable();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        valueEvaluator.evaluate(tuple, valuePointable);
        positionEvaluator.evaluate(tuple, positionPointable);

        if (flagEvaluator != null) {
            flagEvaluator.evaluate(tuple, flagPointable);
        }

        if (PointableHelper.checkAndSetMissingOrNull(result, valuePointable, positionPointable, flagPointable)) {
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

        // Third argument
        boolean isAllSet = false;
        isStopUpdatingResultBoolean = false; // Reset the flag to false for each new tuple
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

            isAllSet = ABooleanSerializerDeserializer.getBoolean(flagBytes, flagStartOffset + 1);
        }

        // Result long value
        long longResult = ATypeHierarchy.getLongValue(functionIdentifier.getName(), 0, valueBytes, valueStartOffset);

        // If any operation returns false, the result should be null
        boolean isSuccessfulOperation;

        // From here onward, we got valid arguments, so let's handle numeric or array position argument
        if (positionTypeTag != ATypeTag.ARRAY) {
            isSuccessfulOperation =
                    applyBitWiseOperationWithNumericAsPosition(longResult, positionBytes, positionStartOffset);
        } else {
            isSuccessfulOperation =
                    applyBitWiseOperationWithArrayAsPosition(longResult, positionBytes, positionStartOffset, isAllSet);
        }

        if (!isSuccessfulOperation) {
            PointableHelper.setNull(result);
            return;
        }

        // Write the result
        resultStorage.reset();
        aBooleanSerde.serialize(resultBoolean, resultStorage.getDataOutput());
        result.set(resultStorage);
    }

    /**
     * This method applies the bitwise operation on the int64 value at the specified positions in
     * the array.
     *
     * @param value The long value
     * @param bytes bytes of the position
     * @param startOffset start offset of the position
     * @param isAllSet is AllSet check flag
     * @return returns {@code true} if all operations are successful, {@code false} otherwise.
     *
     * @throws HyracksDataException HyracksDataException
     */
    private boolean applyBitWiseOperationWithArrayAsPosition(long value, byte[] bytes, int startOffset,
            boolean isAllSet) throws HyracksDataException {

        listAccessor.reset(bytes, startOffset);

        // In case of an empty array, we return the value as is without change
        if (listAccessor.size() == 0) {
            return true;
        }

        try {
            // For each item in the array, apply the bitwise operation at that position
            for (int i = 0; i < listAccessor.size(); i++) {
                listAccessor.getOrWriteItem(i, listItemPointable, resultStorage);

                byte[] itemBytes = listItemPointable.getByteArray();
                int itemStartOffset = listItemPointable.getStartOffset();

                if (!applyBitWiseOperationWithNumericAsPosition(value, itemBytes, itemStartOffset)) {
                    return false;
                }

                // Based on the isAllSet flag, we might want to stop re-calculating the result, this
                // isStopUpdatingResultBoolean keeps an eye on that.
                isStopUpdatingResultBoolean =
                        !isAllSet ? resultBoolean == ABoolean.TRUE : resultBoolean == ABoolean.FALSE;
            }

            return true;
        } catch (IOException ex) {
            throw HyracksDataException.create(ex);
        }
    }

    /**
     * This method applies the bitwise operation on the int64 value at the specified position.
     *
     * @param value the long value
     * @param bytes data bytes
     * @param startOffset start offset
     * @return returns {@code true} if all operations are successful, {@code false} otherwise.
     */
    private boolean applyBitWiseOperationWithNumericAsPosition(long value, byte[] bytes, int startOffset)
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

        // Checks if a certain position is bit 1
        if (!isStopUpdatingResultBoolean) {
            resultBoolean = (value & (1L << (position - 1))) != 0 ? ABoolean.TRUE : ABoolean.FALSE;
        }

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
