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
package org.apache.asterix.om.types.hierachy;

import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;

public class ATypeHierarchy {

    private static final BitSet typePromotionHierachyMap = new BitSet(ATypeTag.TYPE_COUNT * ATypeTag.TYPE_COUNT);
    private static final BitSet typeDemotionHierachyMap = new BitSet(ATypeTag.TYPE_COUNT * ATypeTag.TYPE_COUNT);
    private static final Map<Integer, ITypeConvertComputer> promoteComputerMap = new HashMap<>();
    private static final Map<Integer, Pair<ITypeConvertComputer, ITypeConvertComputer>> demoteComputerMap =
            new HashMap<>();
    private static Map<ATypeTag, Domain> hierarchyDomains = new EnumMap<>(ATypeTag.class);

    // allow type promotion or demotion to the type itself
    static {
        for (int i = 0; i < ATypeTag.TYPE_COUNT; i++) {
            typePromotionHierachyMap.set(i * ATypeTag.TYPE_COUNT + i);
            typeDemotionHierachyMap.set(i * ATypeTag.TYPE_COUNT + i);
        }
    }

    // add default type promotion rules
    static {
        // Promotion (widening): TINYINT -> SMALLINT -> INTEGER -> BIGINT -> FLOAT -> DOUBLE
        // No precision and range loss
        addPromotionRule(ATypeTag.TINYINT, ATypeTag.SMALLINT, IntegerToInt16TypeConvertComputer.getInstance(true));
        addPromotionRule(ATypeTag.TINYINT, ATypeTag.INTEGER, IntegerToInt32TypeConvertComputer.getInstance(true));
        addPromotionRule(ATypeTag.TINYINT, ATypeTag.BIGINT, IntegerToInt64TypeConvertComputer.getInstance());
        addPromotionRule(ATypeTag.SMALLINT, ATypeTag.INTEGER, IntegerToInt32TypeConvertComputer.getInstance(true));
        addPromotionRule(ATypeTag.SMALLINT, ATypeTag.BIGINT, IntegerToInt64TypeConvertComputer.getInstance());
        addPromotionRule(ATypeTag.INTEGER, ATypeTag.BIGINT, IntegerToInt64TypeConvertComputer.getInstance());
        addPromotionRule(ATypeTag.TINYINT, ATypeTag.DOUBLE, IntegerToDoubleTypeConvertComputer.getInstance());
        addPromotionRule(ATypeTag.SMALLINT, ATypeTag.DOUBLE, IntegerToDoubleTypeConvertComputer.getInstance());
        addPromotionRule(ATypeTag.INTEGER, ATypeTag.DOUBLE, IntegerToDoubleTypeConvertComputer.getInstance());
        addPromotionRule(ATypeTag.BIGINT, ATypeTag.DOUBLE, IntegerToDoubleTypeConvertComputer.getInstance());
        addPromotionRule(ATypeTag.FLOAT, ATypeTag.DOUBLE, FloatToDoubleTypeConvertComputer.getInstance());
        addPromotionRule(ATypeTag.TINYINT, ATypeTag.FLOAT, IntegerToFloatTypeConvertComputer.getInstance());
        addPromotionRule(ATypeTag.SMALLINT, ATypeTag.FLOAT, IntegerToFloatTypeConvertComputer.getInstance());
        addPromotionRule(ATypeTag.INTEGER, ATypeTag.FLOAT, IntegerToFloatTypeConvertComputer.getInstance());
        addPromotionRule(ATypeTag.BIGINT, ATypeTag.FLOAT, IntegerToFloatTypeConvertComputer.getInstance());
    }

    static {
        // Demotion (narrowing): DOUBLE -> FLOAT -> BIGINT -> INTEGER -> SMALLINT -> TINYINT
        // Possible precision loss (e.g., FLOAT to INT)
        // 'strict' mode produces an exception if source value is greater than target.MAX or less than target.MIN
        // 'lax' mode does not fail. it returns target.MAX/target.MIN if source value is out of range
        addDemotionRule(ATypeTag.SMALLINT, ATypeTag.TINYINT, IntegerToInt8TypeConvertComputer.getInstance(true),
                IntegerToInt8TypeConvertComputer.getInstance(false));
        addDemotionRule(ATypeTag.INTEGER, ATypeTag.TINYINT, IntegerToInt8TypeConvertComputer.getInstance(true),
                IntegerToInt8TypeConvertComputer.getInstance(false));
        addDemotionRule(ATypeTag.BIGINT, ATypeTag.TINYINT, IntegerToInt8TypeConvertComputer.getInstance(true),
                IntegerToInt8TypeConvertComputer.getInstance(false));
        addDemotionRule(ATypeTag.INTEGER, ATypeTag.SMALLINT, IntegerToInt16TypeConvertComputer.getInstance(true),
                IntegerToInt16TypeConvertComputer.getInstance(false));
        addDemotionRule(ATypeTag.BIGINT, ATypeTag.SMALLINT, IntegerToInt16TypeConvertComputer.getInstance(true),
                IntegerToInt16TypeConvertComputer.getInstance(false));
        addDemotionRule(ATypeTag.BIGINT, ATypeTag.INTEGER, IntegerToInt32TypeConvertComputer.getInstance(true),
                IntegerToInt32TypeConvertComputer.getInstance(false));
        addDemotionRule(ATypeTag.FLOAT, ATypeTag.TINYINT, FloatToInt8TypeConvertComputer.getInstance(true),
                FloatToInt8TypeConvertComputer.getInstance(false));
        addDemotionRule(ATypeTag.FLOAT, ATypeTag.SMALLINT, FloatToInt16TypeConvertComputer.getInstance(true),
                FloatToInt16TypeConvertComputer.getInstance(false));
        addDemotionRule(ATypeTag.FLOAT, ATypeTag.INTEGER, FloatToInt32TypeConvertComputer.getInstance(true),
                FloatToInt32TypeConvertComputer.getInstance(false));
        addDemotionRule(ATypeTag.FLOAT, ATypeTag.BIGINT, FloatToInt64TypeConvertComputer.getInstance(true),
                FloatToInt64TypeConvertComputer.getInstance(false));
        addDemotionRule(ATypeTag.DOUBLE, ATypeTag.TINYINT, DoubleToInt8TypeConvertComputer.getInstance(true),
                DoubleToInt8TypeConvertComputer.getInstance(false));
        addDemotionRule(ATypeTag.DOUBLE, ATypeTag.SMALLINT, DoubleToInt16TypeConvertComputer.getInstance(true),
                DoubleToInt16TypeConvertComputer.getInstance(false));
        addDemotionRule(ATypeTag.DOUBLE, ATypeTag.INTEGER, DoubleToInt32TypeConvertComputer.getInstance(true),
                DoubleToInt32TypeConvertComputer.getInstance(false));
        addDemotionRule(ATypeTag.DOUBLE, ATypeTag.BIGINT, DoubleToInt64TypeConvertComputer.getInstance(true),
                DoubleToInt64TypeConvertComputer.getInstance(false));
        addDemotionRule(ATypeTag.DOUBLE, ATypeTag.FLOAT, DoubleToFloatTypeConvertComputer.getInstance(true),
                DoubleToFloatTypeConvertComputer.getInstance(false));
    }

    static {
        hierarchyDomains.put(ATypeTag.POINT, Domain.SPATIAL);
        hierarchyDomains.put(ATypeTag.LINE, Domain.SPATIAL);
        hierarchyDomains.put(ATypeTag.CIRCLE, Domain.SPATIAL);
        hierarchyDomains.put(ATypeTag.POLYGON, Domain.SPATIAL);
        hierarchyDomains.put(ATypeTag.RECTANGLE, Domain.SPATIAL);
        hierarchyDomains.put(ATypeTag.TINYINT, Domain.NUMERIC);
        hierarchyDomains.put(ATypeTag.SMALLINT, Domain.NUMERIC);
        hierarchyDomains.put(ATypeTag.INTEGER, Domain.NUMERIC);
        hierarchyDomains.put(ATypeTag.BIGINT, Domain.NUMERIC);
        hierarchyDomains.put(ATypeTag.FLOAT, Domain.NUMERIC);
        hierarchyDomains.put(ATypeTag.DOUBLE, Domain.NUMERIC);
        hierarchyDomains.put(ATypeTag.ARRAY, Domain.LIST);
        hierarchyDomains.put(ATypeTag.MULTISET, Domain.LIST);
    }

    public static Domain getTypeDomain(ATypeTag tag) {
        return hierarchyDomains.get(tag);
    }

    public static boolean isSameTypeDomain(ATypeTag tag1, ATypeTag tag2, boolean useListDomain) {
        Domain tagHierarchy1 = hierarchyDomains.get(tag1);
        Domain tagHierarchy2 = hierarchyDomains.get(tag2);
        if (tagHierarchy1 == null || tagHierarchy2 == null) {
            return false;
        }
        if (useListDomain && tagHierarchy1 == Domain.LIST && tagHierarchy2 == Domain.LIST) {
            return true;
        }
        return tagHierarchy1.equals(tagHierarchy2) && !useListDomain;
    }

    private static void addPromotionRule(ATypeTag type1, ATypeTag type2, ITypeConvertComputer promoteComputer) {
        int index = type1.ordinal() * ATypeTag.TYPE_COUNT + type2.ordinal();
        typePromotionHierachyMap.set(index);
        promoteComputerMap.put(index, promoteComputer);
    }

    private static void addDemotionRule(ATypeTag type1, ATypeTag type2, ITypeConvertComputer demoteStrictComputer,
            ITypeConvertComputer demoteLenientComputer) {
        int index = type1.ordinal() * ATypeTag.TYPE_COUNT + type2.ordinal();
        typeDemotionHierachyMap.set(index);
        demoteComputerMap.put(index, Pair.of(demoteStrictComputer, demoteLenientComputer));
    }

    public static ITypeConvertComputer getTypePromoteComputer(ATypeTag type1, ATypeTag type2) {
        if (canPromote(type1, type2)) {
            return promoteComputerMap.get(type1.ordinal() * ATypeTag.TYPE_COUNT + type2.ordinal());
        }
        return null;
    }

    public static ITypeConvertComputer getTypeDemoteComputer(ATypeTag type1, ATypeTag type2, boolean strict) {
        if (canDemote(type1, type2)) {
            Pair<ITypeConvertComputer, ITypeConvertComputer> pair =
                    demoteComputerMap.get(type1.ordinal() * ATypeTag.TYPE_COUNT + type2.ordinal());
            return strict ? pair.getLeft() : pair.getRight();
        }
        return null;
    }

    public static boolean canPromote(ATypeTag type1, ATypeTag type2) {
        return typePromotionHierachyMap.get(type1.ordinal() * ATypeTag.TYPE_COUNT + type2.ordinal());
    }

    public static boolean canDemote(ATypeTag type1, ATypeTag type2) {
        return typeDemotionHierachyMap.get(type1.ordinal() * ATypeTag.TYPE_COUNT + type2.ordinal());
    }

    public static boolean isCompatible(ATypeTag type1, ATypeTag type2) {
        // The type tag ANY is only used at compile time to represent all possibilities.
        // There is no runtime data model instance that has a type tag ANY.
        // If we encounter type tag ANY, we should let it pass at compile time and defer erring to the runtime.
        // Therefore, "type1 == ATypeTag.ANY || type2 == ATypeTag.ANY" works for both compiler and runtime.
        return type1 == ATypeTag.ANY || type2 == ATypeTag.ANY || canPromote(type1, type2) || canPromote(type2, type1);
    }

    // Get an AsterixConstantValue from a source Object
    public static AsterixConstantValue getAsterixConstantValueFromNumericTypeObject(IAObject sourceObject,
            ATypeTag targetTypeTag) throws HyracksDataException {
        return getAsterixConstantValueFromNumericTypeObject(sourceObject, targetTypeTag, false,
                TypeCastingMathFunctionType.NONE);
    }

    // Get an AsterixConstantValue from a source Object
    public static AsterixConstantValue getAsterixConstantValueFromNumericTypeObject(IAObject sourceObject,
            ATypeTag targetTypeTag, boolean strictDemote, TypeCastingMathFunctionType mathFunction)
            throws HyracksDataException {
        ATypeTag sourceTypeTag = sourceObject.getType().getTypeTag();
        if (sourceTypeTag == targetTypeTag) {
            return new AsterixConstantValue(sourceObject);
        }

        // if the constant type and target type does not match, we do a type conversion
        ITypeConvertComputer convertComputer = null;
        if (canPromote(sourceTypeTag, targetTypeTag)) {
            convertComputer = ATypeHierarchy.getTypePromoteComputer(sourceTypeTag, targetTypeTag);
        } else if (canDemote(sourceTypeTag, targetTypeTag)) {
            convertComputer = ATypeHierarchy.getTypeDemoteComputer(sourceTypeTag, targetTypeTag, strictDemote);
        }
        if (convertComputer == null) {
            return null;
        }

        IAObject targetObject = convertComputer.convertType(sourceObject, mathFunction);
        return new AsterixConstantValue(targetObject);
    }

    // Type Casting from source Object to an Object with Target type
    public static IAObject convertNumericTypeObject(IAObject sourceObject, ATypeTag targetTypeTag)
            throws HyracksDataException {
        return convertNumericTypeObject(sourceObject, targetTypeTag, false, TypeCastingMathFunctionType.NONE);
    }

    // Type Casting from source Object to an Object with Target type
    public static IAObject convertNumericTypeObject(IAObject sourceObject, ATypeTag targetTypeTag, boolean strictDemote,
            TypeCastingMathFunctionType mathFunction) throws HyracksDataException {
        ATypeTag sourceTypeTag = sourceObject.getType().getTypeTag();
        if (sourceTypeTag == targetTypeTag) {
            return sourceObject;
        }

        ITypeConvertComputer convertComputer = null;
        if (canPromote(sourceTypeTag, targetTypeTag)) {
            convertComputer = ATypeHierarchy.getTypePromoteComputer(sourceTypeTag, targetTypeTag);
        } else if (canDemote(sourceTypeTag, targetTypeTag)) {
            convertComputer = ATypeHierarchy.getTypeDemoteComputer(sourceTypeTag, targetTypeTag, strictDemote);
        }
        if (convertComputer == null) {
            throw new RuntimeDataException(ErrorCode.TYPE_CONVERT, sourceTypeTag, targetTypeTag);
        }

        return convertComputer.convertType(sourceObject, mathFunction);
    }

    // convert a numeric value in a byte array to the target type value
    public static void convertNumericTypeByteArray(byte[] sourceByteArray, int s1, int l1, ATypeTag targetTypeTag,
            DataOutput out) throws IOException {
        convertNumericTypeByteArray(sourceByteArray, s1, l1, targetTypeTag, out, false);
    }

    // convert a numeric value in a byte array to the target type value
    public static void convertNumericTypeByteArray(byte[] sourceByteArray, int s1, int l1, ATypeTag targetTypeTag,
            DataOutput out, boolean strictDemote) throws IOException {
        ATypeTag sourceTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(sourceByteArray[s1]);

        if (sourceTypeTag != targetTypeTag) {
            // source tag can be promoted to target tag (e.g. tag1: SMALLINT, tag2: INTEGER)
            if (canPromote(sourceTypeTag, targetTypeTag)) {
                ITypeConvertComputer convertComputer =
                        ATypeHierarchy.getTypePromoteComputer(sourceTypeTag, targetTypeTag);
                convertComputer.convertType(sourceByteArray, s1 + 1, l1 - 1, out);
                // source tag can be demoted to target tag
            } else if (canDemote(sourceTypeTag, targetTypeTag)) {
                ITypeConvertComputer convertComputer =
                        ATypeHierarchy.getTypeDemoteComputer(sourceTypeTag, targetTypeTag, strictDemote);
                convertComputer.convertType(sourceByteArray, s1 + 1, l1 - 1, out);
            } else {
                throw new RuntimeDataException(ErrorCode.TYPE_CONVERT, sourceTypeTag, targetTypeTag);
            }
        }
    }

    // Get an INT value from numeric types array. We assume the first byte contains the type tag.
    public static int getIntegerValue(String name, int argIndex, byte[] bytes, int offset) throws HyracksDataException {
        return getIntegerValue(name, argIndex, bytes, offset, false);
    }

    // Get an INT value from numeric types array. We assume the first byte contains the type tag.
    public static int getIntegerValue(String name, int argIndex, byte[] bytes, int offset, boolean strictDemote)
            throws HyracksDataException {
        return getIntegerValueWithDifferentTypeTagPosition(name, argIndex, bytes, offset + 1, offset, strictDemote);
    }

    // Get an INT value from numeric types array. We assume the specific location of a byte array contains the type tag.
    public static int getIntegerValueWithDifferentTypeTagPosition(String name, int argIndex, byte[] bytes, int offset,
            int typeTagPosition) throws HyracksDataException {
        return getIntegerValueWithDifferentTypeTagPosition(name, argIndex, bytes, offset, typeTagPosition, false);
    }

    // Get an INT value from numeric types array. We assume the specific location of a byte array contains the type tag.
    public static int getIntegerValueWithDifferentTypeTagPosition(String name, int argIndex, byte[] bytes, int offset,
            int typeTagPosition, boolean strictDemote) throws HyracksDataException {
        ATypeTag sourceTypeTag = ATypeTag.VALUE_TYPE_MAPPING[bytes[typeTagPosition]];
        if (sourceTypeTag == null) {
            throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, name, argIndex);
        }
        switch (sourceTypeTag) {
            case INTEGER:
                return IntegerPointable.getInteger(bytes, offset);
            case TINYINT:
            case SMALLINT:
            case BIGINT:
                return (int) IntegerToInt32TypeConvertComputer.getInstance(strictDemote).convertIntegerType(bytes,
                        offset, sourceTypeTag, ATypeTag.INTEGER);
            case FLOAT:
                return FloatToInt32TypeConvertComputer.getInstance(strictDemote).convertType(bytes, offset);
            case DOUBLE:
                return DoubleToInt32TypeConvertComputer.getInstance(strictDemote).convertType(bytes, offset);
            default:
                throw createTypeMismatchException(name, argIndex, sourceTypeTag, ATypeTag.TINYINT, ATypeTag.SMALLINT,
                        ATypeTag.INTEGER, ATypeTag.BIGINT, ATypeTag.FLOAT, ATypeTag.DOUBLE);
        }
    }

    // Get a LONG (bigint) value from numeric types array. We assume the first byte contains the type tag.
    public static long getLongValue(String name, int argIndex, byte[] bytes, int offset) throws HyracksDataException {
        return getLongValue(name, argIndex, bytes, offset, false);
    }

    // Get a LONG (bigint) value from numeric types array. We assume the first byte contains the type tag.
    public static long getLongValue(String name, int argIndex, byte[] bytes, int offset, boolean strictDemote)
            throws HyracksDataException {
        return getLongValueWithDifferentTypeTagPosition(name, argIndex, bytes, offset + 1, offset, strictDemote);
    }

    // Get a LONG (bigint) value from numeric types array. We assume the specific location of a byte array
    // contains the type tag.
    private static long getLongValueWithDifferentTypeTagPosition(String name, int argIndex, byte[] bytes, int offset,
            int typeTagPosition, boolean strictDemote) throws HyracksDataException {
        ATypeTag sourceTypeTag = ATypeTag.VALUE_TYPE_MAPPING[bytes[typeTagPosition]];
        if (sourceTypeTag == null) {
            throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, name, argIndex);
        }
        switch (sourceTypeTag) {
            case BIGINT:
                return LongPointable.getLong(bytes, offset);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return IntegerToInt64TypeConvertComputer.getInstance().convertIntegerType(bytes, offset, sourceTypeTag,
                        ATypeTag.BIGINT);
            case FLOAT:
                return FloatToInt64TypeConvertComputer.getInstance(strictDemote).convertType(bytes, offset);
            case DOUBLE:
                return DoubleToInt64TypeConvertComputer.getInstance(strictDemote).convertType(bytes, offset);
            default:
                throw createTypeMismatchException(name, argIndex, sourceTypeTag, ATypeTag.TINYINT, ATypeTag.SMALLINT,
                        ATypeTag.INTEGER, ATypeTag.BIGINT, ATypeTag.FLOAT, ATypeTag.DOUBLE);
        }
    }

    // Get a DOUBLE value from numeric types array. We assume the first byte contains the type tag.
    public static double getDoubleValue(String name, int argIndex, byte[] bytes, int offset)
            throws HyracksDataException {
        return getDoubleValueWithDifferentTypeTagPosition(name, argIndex, bytes, offset + 1, offset);
    }

    // Get a DOUBLE value from numeric types array. We assume the specific location of a byte array contains the type tag.
    private static double getDoubleValueWithDifferentTypeTagPosition(String name, int argIndex, byte[] bytes,
            int offset, int typeTagPosition) throws HyracksDataException {
        ATypeTag sourceTypeTag = ATypeTag.VALUE_TYPE_MAPPING[bytes[typeTagPosition]];
        if (sourceTypeTag == null) {
            throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, name, argIndex);
        }
        switch (sourceTypeTag) {
            case DOUBLE:
                return DoublePointable.getDouble(bytes, offset);
            case FLOAT:
                return FloatToDoubleTypeConvertComputer.getInstance().convertType(bytes, offset);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return IntegerToDoubleTypeConvertComputer.getInstance().convertType(bytes, offset, sourceTypeTag);
            default:
                throw createTypeMismatchException(name, argIndex, sourceTypeTag, ATypeTag.TINYINT, ATypeTag.SMALLINT,
                        ATypeTag.INTEGER, ATypeTag.BIGINT, ATypeTag.FLOAT, ATypeTag.DOUBLE);
        }
    }

    /**
     * Applies certain math function (e.g., ceil or floor) to a double value and returns that value.
     */
    public static double applyMathFunctionToDoubleValue(IAObject sourceObject,
            TypeCastingMathFunctionType mathFunction) {
        switch (mathFunction) {
            case CEIL:
                return Math.ceil(((ADouble) sourceObject).getDoubleValue());
            case FLOOR:
                return Math.floor(((ADouble) sourceObject).getDoubleValue());
            default:
                return ((ADouble) sourceObject).getDoubleValue();
        }
    }

    /**
     * Applies certain math function (e.g., ceil or floor) to a float value and returns that value.
     */
    public static float applyMathFunctionToFloatValue(IAObject sourceObject, TypeCastingMathFunctionType mathFunction) {
        switch (mathFunction) {
            case CEIL:
                return (float) Math.ceil(((AFloat) sourceObject).getFloatValue());
            case FLOOR:
                return (float) Math.floor(((AFloat) sourceObject).getFloatValue());
            default:
                return ((AFloat) sourceObject).getFloatValue();
        }
    }

    private static RuntimeDataException createTypeMismatchException(String functionName, int argIdx,
            ATypeTag actualTypeTag, ATypeTag... expectedTypeTags) {
        return new RuntimeDataException(ErrorCode.TYPE_MISMATCH_FUNCTION, functionName,
                ExceptionUtil.indexToPosition(argIdx), ExceptionUtil.toExpectedTypeString(expectedTypeTags),
                actualTypeTag);
    }

    public enum Domain {
        SPATIAL,
        NUMERIC,
        LIST,
        ANY
    }

    // Type-casting mathFunction that will be used to type-cast a FLOAT or a DOUBLE value into an INTEGER value.
    public enum TypeCastingMathFunctionType {
        CEIL,
        FLOOR,
        NONE
    }
}
