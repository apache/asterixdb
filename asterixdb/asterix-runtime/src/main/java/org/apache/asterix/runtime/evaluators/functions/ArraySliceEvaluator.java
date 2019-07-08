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

import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;

import java.io.IOException;

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * <pre>
 * array_slice(array, start, end) returns a subset of the {@code array} containing the elements from position
 * {@code start} to position {@code end -1}. The {@code end} argument is optional. if {@code end} is not provided,
 * the returned subset of the {@code array} contains the elements from {@code start} position to the end of
 * {@code array}. The array index starts at {@code 0}. The element at {@code start} is included while the element
 * at {@code end} is not included. Negative positions are counted backwards from the end of the array.
 *
 * Examples:
 * array_slice([1, 2, 3, 4], 1, 3) will return [2, 3].
 * array_slice([1, 2, 3, 4], -3, 3) will return [2, 3].
 * array_slice([1, 2, 3, 4], 1) will return [2, 3, 4].
 *
 * It throws an error at compile time if the number of arguments is greater than {@code 3} or less than {@code 2}.
 *
 * {@code NULL} is returned if:
 *  - {@code array} is not an array.
 *  - {@code start} or {@code end} is not valid numbers. 1, 2, 3.0 are accepted but 3.2 is not accepted.
 *  - {@code start} or {@code end} is greater than the length of the {@code array}.
 *  - {@code end} is smaller than {@code start}.
 * </pre>
 */

class ArraySliceEvaluator extends AbstractScalarEval {

    // List type
    private final IAType inputListType;

    // Storage
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();

    // Evaluators and pointables
    private final IScalarEvaluator listEval;
    private final IScalarEvaluator startPositionEval;
    private IScalarEvaluator endPositionEval;
    private final IPointable listPointable = new VoidPointable();
    private final IPointable startPositionPointable = new VoidPointable();
    private IPointable endPositionPointable;

    // Accessors
    private final ListAccessor listAccessor = new ListAccessor();

    // List Builders
    private final IAsterixListBuilder orderedListBuilder = new OrderedListBuilder();
    private final IAsterixListBuilder unorderedListBuilder = new UnorderedListBuilder();

    // Constructor
    ArraySliceEvaluator(IScalarEvaluatorFactory[] argEvalFactories, IEvaluatorContext ctx, SourceLocation sourceLoc,
            FunctionIdentifier functionIdentifier, IAType inputListType) throws HyracksDataException {
        // Source location
        super(sourceLoc, functionIdentifier);

        // List type
        this.inputListType = inputListType;

        // Evaluators
        listEval = argEvalFactories[0].createScalarEvaluator(ctx);
        startPositionEval = argEvalFactories[1].createScalarEvaluator(ctx);

        // Check for optional parameter
        if (argEvalFactories.length > 2) {
            endPositionEval = argEvalFactories[2].createScalarEvaluator(ctx);
            endPositionPointable = new VoidPointable();
        }
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {

        // Evaluate
        listEval.evaluate(tuple, listPointable);
        startPositionEval.evaluate(tuple, startPositionPointable);
        if (endPositionEval != null) {
            endPositionEval.evaluate(tuple, endPositionPointable);
        }

        if (PointableHelper.checkAndSetMissingOrNull(result, listPointable, startPositionPointable,
                endPositionPointable)) {
            return;
        }

        // Positions
        int startPositionValue;
        int endPositionValue = 0;

        // Data bytes, offsets and type tags
        byte[] listBytes = listPointable.getByteArray();
        int listOffset = listPointable.getStartOffset();
        ATypeTag listTypetag = ATYPETAGDESERIALIZER.deserialize(listBytes[listOffset]);
        byte[] startPositionBytes = startPositionPointable.getByteArray();
        int startPositionOffset = startPositionPointable.getStartOffset();
        ATypeTag startPositionTypeTag = ATYPETAGDESERIALIZER.deserialize(startPositionBytes[startPositionOffset]);

        // Invalid types checks
        if (!listTypetag.isListType() || !ATypeHierarchy.isCompatible(startPositionTypeTag, ATypeTag.DOUBLE)) {
            PointableHelper.setNull(result);
            return;
        }

        // List accessor
        listAccessor.reset(listBytes, listOffset);

        // Optional parameter
        byte[] endPositionBytes = null;
        int endPositionOffset = 0;
        ATypeTag endPositionTypeTag;

        // End position available or use list length as end position
        if (endPositionEval != null) {
            endPositionBytes = endPositionPointable.getByteArray();
            endPositionOffset = endPositionPointable.getStartOffset();
            endPositionTypeTag = ATYPETAGDESERIALIZER.deserialize(endPositionBytes[endPositionOffset]);

            // Invalid types checks
            if (!ATypeHierarchy.isCompatible(endPositionTypeTag, ATypeTag.DOUBLE)) {
                PointableHelper.setNull(result);
                return;
            }
        } else {
            // Use list length as end position
            endPositionValue = listAccessor.size();
            endPositionTypeTag = ATypeTag.BIGINT;
        }

        // From here onward, all arguments are available and compatible
        // Get the position value
        try {
            startPositionValue = getValue(startPositionBytes, startPositionOffset + 1, startPositionTypeTag);
            endPositionValue = endPositionEval != null
                    ? getValue(endPositionBytes, endPositionOffset + 1, endPositionTypeTag) : endPositionValue;
        } catch (HyracksDataException ignored) { // NOSONAR: Ignore the exception, invalid number returns null
            PointableHelper.setNull(result);
            return;
        }

        // Since we accept negative values for positions, we need to convert them appropriately to positives before
        // we compare the start and end positions. (e.g. length = 4, start = -1 -> start = 4 + (-1) = 3)
        startPositionValue = startPositionValue < 0 ? listAccessor.size() + startPositionValue : startPositionValue;
        endPositionValue = endPositionValue < 0 ? listAccessor.size() + endPositionValue : endPositionValue;

        // Check arguments validity
        if (!isValidArguments(listAccessor.size(), startPositionValue, endPositionValue)) {
            PointableHelper.setNull(result);
            return;
        }

        // From here onward, all arguments are valid
        // List builder & collection type
        AbstractCollectionType collectionType;
        IAsterixListBuilder listBuilder =
                listAccessor.getListType() == ATypeTag.ARRAY ? orderedListBuilder : unorderedListBuilder;

        // Unknown list type
        if (!inputListType.getTypeTag().isListType()) {
            // Get the list item type using the type tag
            ATypeTag listItemTypeTag = listAccessor.getItemType();
            IAType listItemType = TypeTagUtil.getBuiltinTypeByTag(listItemTypeTag);

            // List of type listItemType
            if (listAccessor.getListType() == ATypeTag.ARRAY) {
                collectionType = new AOrderedListType(listItemType, listItemType.getTypeName());
            } else {
                collectionType = new AUnorderedListType(listItemType, listItemType.getTypeName());
            }
        }
        // Known list type, use it directly
        else {
            collectionType = (AbstractCollectionType) inputListType;
        }

        // Builder list type
        listBuilder.reset(collectionType);

        try {
            // Create the subset list based on the positions
            for (int i = startPositionValue; i < endPositionValue; i++) {
                resultStorage.reset();
                listAccessor.writeItem(i, resultStorage.getDataOutput());
                listBuilder.addItem(resultStorage);
            }
        } catch (IOException ex) {
            throw HyracksDataException.create(ex);
        }

        // Final result
        resultStorage.reset();
        listBuilder.write(resultStorage.getDataOutput(), true);
        result.set(resultStorage);
    }

    // Get the value
    private int getValue(byte[] data, int offset, ATypeTag typeTag) throws HyracksDataException {
        // Using double since we accept values like 3.0, but not 3.5
        double value;

        // Value based on type tag
        switch (typeTag) {
            case TINYINT:
                value = AInt8SerializerDeserializer.getByte(data, offset);
                break;
            case SMALLINT:
                value = AInt16SerializerDeserializer.getShort(data, offset);
                break;
            case INTEGER:
                value = AInt32SerializerDeserializer.getInt(data, offset);
                break;
            case BIGINT:
                value = AInt64SerializerDeserializer.getLong(data, offset);
                break;
            case FLOAT:
                value = AFloatSerializerDeserializer.getFloat(data, offset);
                break;
            case DOUBLE:
                value = ADoubleSerializerDeserializer.getDouble(data, offset);
                break;
            default:
                throw new UnsupportedItemTypeException(sourceLoc, functionIdentifier, typeTag.serialize());
        }

        // Values like 1, 2, 3.0 are ok, but 0.3 and 3.5 are not accepted, also handle NaN and INF/-INF
        if (Double.isNaN(value) || Double.isInfinite(value) || value > Math.floor(value)) {
            throw new InvalidDataFormatException(sourceLoc, functionIdentifier, typeTag.serialize());
        }

        return (int) value;
    }

    /**
     * Ensures that the positions used are valid
     *
     * @param listLength list length
     * @param startPosition start position value
     * @param endPosition end position value
     *
     * @return {@code true} if all conditions are valid, otherwise {@code false}
     */
    private boolean isValidArguments(double listLength, double startPosition, double endPosition) {

        // Negative values check (negative positions already adjusted, if a value is still negative then it's
        // greater than the list length)
        if (startPosition < 0 || endPosition < 0) {
            return false;
        }

        // Length vs Position check
        if (startPosition > listLength - 1 || endPosition > listLength) {
            return false;
        }

        // Value validity check (1, 2, 3.0 are accepted, but 3.2 is not)
        if (startPosition > Math.floor(startPosition) || endPosition > Math.floor(endPosition)) {
            return false;
        }

        // Start vs end position check (start position can't be greater than end position)
        if (startPosition > endPosition) {
            return false;
        }

        // All conditions passed
        return true;
    }
}
