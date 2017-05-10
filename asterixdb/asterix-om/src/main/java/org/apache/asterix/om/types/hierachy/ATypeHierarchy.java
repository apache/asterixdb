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
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;

public class ATypeHierarchy {

    private static BitSet typePromotionHierachyMap = new BitSet(ATypeTag.TYPE_COUNT * ATypeTag.TYPE_COUNT);
    private static BitSet typeDemotionHierachyMap = new BitSet(ATypeTag.TYPE_COUNT * ATypeTag.TYPE_COUNT);
    private static HashMap<Integer, ITypeConvertComputer> promoteComputerMap = new HashMap<Integer, ITypeConvertComputer>();
    private static HashMap<Integer, ITypeConvertComputer> demoteComputerMap = new HashMap<Integer, ITypeConvertComputer>();
    private static Map<ATypeTag, Domain> hierarchyDomains = new HashMap<ATypeTag, Domain>();
    private static ITypeConvertComputer convertComputer;

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
        addPromotionRule(ATypeTag.TINYINT, ATypeTag.SMALLINT, IntegerToInt16TypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.TINYINT, ATypeTag.INTEGER, IntegerToInt32TypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.TINYINT, ATypeTag.BIGINT, IntegerToInt64TypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.SMALLINT, ATypeTag.INTEGER, IntegerToInt32TypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.SMALLINT, ATypeTag.BIGINT, IntegerToInt64TypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.INTEGER, ATypeTag.BIGINT, IntegerToInt64TypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.TINYINT, ATypeTag.DOUBLE, IntegerToDoubleTypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.SMALLINT, ATypeTag.DOUBLE, IntegerToDoubleTypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.INTEGER, ATypeTag.DOUBLE, IntegerToDoubleTypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.BIGINT, ATypeTag.DOUBLE, IntegerToDoubleTypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.FLOAT, ATypeTag.DOUBLE, FloatToDoubleTypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.TINYINT, ATypeTag.FLOAT, IntegerToFloatTypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.SMALLINT, ATypeTag.FLOAT, IntegerToFloatTypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.INTEGER, ATypeTag.FLOAT, IntegerToFloatTypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.BIGINT, ATypeTag.FLOAT, IntegerToFloatTypeConvertComputer.INSTANCE);
    }

    static {
        // Demotion (narrowing): DOUBLE -> FLOAT -> BIGINT -> INTEGER -> SMALLINT -> TINYINT
        // Possible precision loss (e.g., FLOAT to INT)
        // This may produce an exception (if source value is greater than target.MAX or less than target.MIN)
        addDemotionRule(ATypeTag.SMALLINT, ATypeTag.TINYINT, IntegerToInt8TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.INTEGER, ATypeTag.TINYINT, IntegerToInt8TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.BIGINT, ATypeTag.TINYINT, IntegerToInt8TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.INTEGER, ATypeTag.SMALLINT, IntegerToInt16TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.BIGINT, ATypeTag.SMALLINT, IntegerToInt16TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.BIGINT, ATypeTag.INTEGER, IntegerToInt32TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.FLOAT, ATypeTag.TINYINT, FloatToInt8TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.FLOAT, ATypeTag.SMALLINT, FloatToInt16TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.FLOAT, ATypeTag.INTEGER, FloatToInt32TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.FLOAT, ATypeTag.BIGINT, FloatToInt64TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.DOUBLE, ATypeTag.TINYINT, DoubleToInt8TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.DOUBLE, ATypeTag.SMALLINT, DoubleToInt16TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.DOUBLE, ATypeTag.INTEGER, DoubleToInt32TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.DOUBLE, ATypeTag.BIGINT, DoubleToInt64TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.DOUBLE, ATypeTag.FLOAT, DoubleToFloatTypeConvertComputer.INSTANCE);
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

    public static void addPromotionRule(ATypeTag type1, ATypeTag type2, ITypeConvertComputer promoteComputer) {
        int index = type1.ordinal() * ATypeTag.TYPE_COUNT + type2.ordinal();
        typePromotionHierachyMap.set(index);
        promoteComputerMap.put(index, promoteComputer);
    }

    public static void addDemotionRule(ATypeTag type1, ATypeTag type2, ITypeConvertComputer demoteComputer) {
        int index = type1.ordinal() * ATypeTag.TYPE_COUNT + type2.ordinal();
        typeDemotionHierachyMap.set(index);
        demoteComputerMap.put(index, demoteComputer);
    }

    public static ITypeConvertComputer getTypePromoteComputer(ATypeTag type1, ATypeTag type2) {
        if (canPromote(type1, type2)) {
            return promoteComputerMap.get(type1.ordinal() * ATypeTag.TYPE_COUNT + type2.ordinal());
        }
        return null;
    }

    public static ITypeConvertComputer getTypeDemoteComputer(ATypeTag type1, ATypeTag type2) {
        if (canDemote(type1, type2)) {
            return demoteComputerMap.get(type1.ordinal() * ATypeTag.TYPE_COUNT + type2.ordinal());
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
            ATypeTag targetTypeTag) throws AlgebricksException {
        ATypeTag sourceTypeTag = sourceObject.getType().getTypeTag();
        AsterixConstantValue asterixNewConstantValue = null;
        short tmpShortValue;
        int tmpIntValue;
        long tmpLongValue;
        float tmpFloatValue;
        double tmpDoubleValue;

        // if the constant type and target type does not match, we do a type conversion
        if (sourceTypeTag != targetTypeTag) {

            switch (targetTypeTag) {
                //Target Field Type:BIGINT
                case BIGINT:

                    // Change the Constant Type to BIGINT Type
                    switch (sourceTypeTag) {
                        case TINYINT:
                            asterixNewConstantValue = new AsterixConstantValue(
                                    new AInt64((long) ((AInt8) sourceObject).getByteValue()));
                            break;

                        case SMALLINT:
                            asterixNewConstantValue = new AsterixConstantValue(
                                    new AInt64((long) ((AInt16) sourceObject).getShortValue()));
                            break;

                        case INTEGER:
                            asterixNewConstantValue = new AsterixConstantValue(
                                    new AInt64((long) ((AInt32) sourceObject).getIntegerValue()));
                            break;

                        case FLOAT:
                            tmpFloatValue = ((AFloat) sourceObject).getFloatValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.FLOAT, ATypeTag.BIGINT, tmpFloatValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt64((long) tmpFloatValue));
                            break;

                        case DOUBLE:
                            tmpDoubleValue = ((ADouble) sourceObject).getDoubleValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.DOUBLE, ATypeTag.BIGINT, tmpDoubleValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt64((long) tmpDoubleValue));
                            break;

                        default:
                            break;
                    }

                    break;

                //Target Field Type:INTEGER
                case INTEGER:

                    // Change the Constant Type to INTEGER Type
                    switch (sourceTypeTag) {
                        case TINYINT:
                            asterixNewConstantValue = new AsterixConstantValue(
                                    new AInt32(((AInt8) sourceObject).getByteValue()));
                            break;

                        case SMALLINT:
                            asterixNewConstantValue = new AsterixConstantValue(
                                    new AInt32(((AInt16) sourceObject).getShortValue()));
                            break;

                        case BIGINT:
                            tmpLongValue = ((AInt64) sourceObject).getLongValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.BIGINT, ATypeTag.INTEGER, tmpLongValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt32((int) tmpLongValue));
                            break;

                        case FLOAT:
                            tmpFloatValue = ((AFloat) sourceObject).getFloatValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.FLOAT, ATypeTag.INTEGER, tmpFloatValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt32((int) tmpFloatValue));
                            break;

                        case DOUBLE:
                            tmpDoubleValue = ((ADouble) sourceObject).getDoubleValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.FLOAT, ATypeTag.INTEGER, tmpDoubleValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt32((int) tmpDoubleValue));
                            break;

                        default:
                            break;
                    }

                    break;

                //Target Field Type:TINYINT
                case TINYINT:

                    // Change the Constant Type to TINYINT Type
                    switch (sourceTypeTag) {
                        case SMALLINT:
                            tmpShortValue = ((AInt16) sourceObject).getShortValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.SMALLINT, ATypeTag.TINYINT, tmpShortValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt8((byte) tmpShortValue));
                            break;

                        case INTEGER:
                            tmpIntValue = ((AInt32) sourceObject).getIntegerValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.INTEGER, ATypeTag.TINYINT, tmpIntValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt8((byte) tmpIntValue));
                            break;

                        case BIGINT:
                            tmpLongValue = ((AInt64) sourceObject).getLongValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.BIGINT, ATypeTag.TINYINT, tmpLongValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt8((byte) tmpLongValue));
                            break;

                        case FLOAT:
                            tmpFloatValue = ((AFloat) sourceObject).getFloatValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.FLOAT, ATypeTag.TINYINT, tmpFloatValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt8((byte) tmpFloatValue));
                            break;

                        case DOUBLE:
                            tmpDoubleValue = ((ADouble) sourceObject).getDoubleValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.DOUBLE, ATypeTag.TINYINT, tmpDoubleValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt8((byte) tmpDoubleValue));
                            break;

                        default:
                            break;
                    }
                    break;

                //Target Field Type:SMALLINT
                case SMALLINT:
                    // Change the Constant Type to SMALLINT Type
                    switch (sourceTypeTag) {
                        case TINYINT:
                            asterixNewConstantValue = new AsterixConstantValue(
                                    new AInt16(((AInt8) sourceObject).getByteValue()));
                            break;

                        case INTEGER:
                            tmpIntValue = ((AInt32) sourceObject).getIntegerValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.INTEGER, ATypeTag.SMALLINT, tmpIntValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt16((short) tmpIntValue));
                            break;

                        case BIGINT:
                            tmpLongValue = ((AInt64) sourceObject).getLongValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.BIGINT, ATypeTag.SMALLINT, tmpLongValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt16((short) tmpLongValue));
                            break;

                        case FLOAT:
                            tmpFloatValue = ((AFloat) sourceObject).getFloatValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.FLOAT, ATypeTag.SMALLINT, tmpFloatValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt16((short) tmpFloatValue));
                            break;

                        case DOUBLE:
                            tmpDoubleValue = ((ADouble) sourceObject).getDoubleValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.DOUBLE, ATypeTag.SMALLINT, tmpDoubleValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt16((short) tmpDoubleValue));
                            break;

                        default:
                            break;
                    }
                    break;

                //Target Field Type:FLOAT
                case FLOAT:
                    // Change the Constant Type to FLOAT Type
                    switch (sourceTypeTag) {
                        case TINYINT:
                            asterixNewConstantValue = new AsterixConstantValue(
                                    new AFloat(((AInt8) sourceObject).getByteValue()));
                            break;

                        case SMALLINT:
                            asterixNewConstantValue = new AsterixConstantValue(
                                    new AFloat(((AInt16) sourceObject).getShortValue()));
                            break;

                        case INTEGER:
                            asterixNewConstantValue = new AsterixConstantValue(
                                    new AFloat(((AInt32) sourceObject).getIntegerValue()));
                            break;

                        case BIGINT:
                            asterixNewConstantValue = new AsterixConstantValue(
                                    new AFloat(((AInt64) sourceObject).getLongValue()));
                            break;

                        case DOUBLE:
                            tmpDoubleValue = ((ADouble) sourceObject).getDoubleValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.DOUBLE, ATypeTag.FLOAT, tmpDoubleValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AFloat((float) tmpDoubleValue));
                            break;

                        default:
                            break;
                    }
                    break;

                //Target Field Type:DOUBLE
                case DOUBLE:
                    // Change the Constant Type to DOUBLE Type
                    switch (sourceTypeTag) {
                        case TINYINT:
                            asterixNewConstantValue = new AsterixConstantValue(
                                    new ADouble(((AInt8) sourceObject).getByteValue()));
                            break;

                        case SMALLINT:
                            asterixNewConstantValue = new AsterixConstantValue(
                                    new ADouble(((AInt16) sourceObject).getShortValue()));
                            break;

                        case INTEGER:
                            asterixNewConstantValue = new AsterixConstantValue(
                                    new ADouble(((AInt32) sourceObject).getIntegerValue()));
                            break;

                        case BIGINT:
                            asterixNewConstantValue = new AsterixConstantValue(
                                    new ADouble(((AInt64) sourceObject).getLongValue()));
                            break;

                        case FLOAT:
                            asterixNewConstantValue = new AsterixConstantValue(
                                    new ADouble(((AFloat) sourceObject).getFloatValue()));
                            break;

                        default:
                            break;
                    }
                    break;

                default:
                    break;
            }

            return asterixNewConstantValue;

        } else {

            return new AsterixConstantValue(sourceObject);
        }

    }

    // checks whether the source value is within the range of the target type
    private static void valueSanitycheck(ATypeTag sourceType, ATypeTag targetType, double sourceValue)
            throws AlgebricksException {
        boolean canConvert = true;

        switch (targetType) {
            case TINYINT:
                if (sourceValue > Byte.MAX_VALUE || sourceValue < Byte.MIN_VALUE) {
                    canConvert = false;
                }
                break;

            case SMALLINT:
                if (sourceValue > Short.MAX_VALUE || sourceValue < Short.MIN_VALUE) {
                    canConvert = false;
                }
                break;
            case INTEGER:
                if (sourceValue > Integer.MAX_VALUE || sourceValue < Integer.MIN_VALUE) {
                    canConvert = false;
                }
                break;
            case BIGINT:
                if (sourceValue > Long.MAX_VALUE || sourceValue < Long.MIN_VALUE) {
                    canConvert = false;
                }
                break;
            case FLOAT:
                if (sourceValue > Float.MAX_VALUE || sourceValue < Float.MIN_VALUE) {
                    canConvert = false;
                }
                break;
            default:
                break;
        }

        if (!canConvert) {
            throw new AlgebricksException("Can't cast a value: " + sourceValue + " from " + sourceType + " type to "
                    + targetType + " type because of the out-of-range error.");
        }
    }

    // Type Casting from source Object to an Object with Target type
    public static IAObject convertNumericTypeObject(IAObject sourceObject, ATypeTag targetTypeTag)
            throws AsterixException {
        ATypeTag sourceTypeTag = sourceObject.getType().getTypeTag();

        switch (sourceTypeTag) {
            case TINYINT:
                switch (targetTypeTag) {
                    case TINYINT:
                        return sourceObject;
                    case SMALLINT:
                        return new AInt16(((AInt8) sourceObject).getByteValue());
                    case INTEGER:
                        return new AInt32(((AInt8) sourceObject).getByteValue());
                    case BIGINT:
                        return new AInt64((long) ((AInt8) sourceObject).getByteValue());
                    case FLOAT:
                        return new AFloat(((AInt8) sourceObject).getByteValue());
                    case DOUBLE:
                        return new ADouble(((AInt8) sourceObject).getByteValue());
                    default:
                        throw new AsterixException(
                                "Can't convert the " + sourceTypeTag + " type to the " + targetTypeTag + " type.");
                }
            case SMALLINT:
                switch (targetTypeTag) {
                    case TINYINT:
                        // an exception can happen because of a type demotion from SMALLINT to TINYINT
                        return new AInt8((byte) ((AInt16) sourceObject).getShortValue());
                    case SMALLINT:
                        return sourceObject;
                    case INTEGER:
                        return new AInt32(((AInt16) sourceObject).getShortValue());
                    case BIGINT:
                        return new AInt64((long) ((AInt16) sourceObject).getShortValue());
                    case FLOAT:
                        return new AFloat(((AInt16) sourceObject).getShortValue());
                    case DOUBLE:
                        return new ADouble(((AInt16) sourceObject).getShortValue());
                    default:
                        throw new AsterixException(
                                "Can't convert the " + sourceTypeTag + " type to the " + targetTypeTag + " type.");
                }

            case INTEGER:
                switch (targetTypeTag) {
                    case TINYINT:
                        // an exception can happen because of a type demotion from INTEGER to TINYINT
                        return new AInt8(((AInt32) sourceObject).getIntegerValue().byteValue());
                    case SMALLINT:
                        // an exception can happen because of a type demotion from INTEGER to SMALLINT
                        return new AInt16(((AInt32) sourceObject).getIntegerValue().shortValue());
                    case INTEGER:
                        return sourceObject;
                    case BIGINT:
                        return new AInt64((long) ((AInt32) sourceObject).getIntegerValue());
                    case FLOAT:
                        return new AFloat(((AInt32) sourceObject).getIntegerValue());
                    case DOUBLE:
                        return new ADouble(((AInt32) sourceObject).getIntegerValue());
                    default:
                        throw new AsterixException(
                                "Can't convert the " + sourceTypeTag + " type to the " + targetTypeTag + " type.");
                }

            case BIGINT:
                switch (targetTypeTag) {
                    case TINYINT:
                        // an exception can happen because of a type demotion from BIGINT to TINYINT
                        return new AInt8((byte) ((AInt64) sourceObject).getLongValue());
                    case SMALLINT:
                        // an exception can happen because of a type demotion from BIGINT to SMALLINT
                        return new AInt16((short) ((AInt64) sourceObject).getLongValue());
                    case INTEGER:
                        // an exception can happen because of a type demotion from BIGINT to INTEGER
                        return new AInt32((int) ((AInt64) sourceObject).getLongValue());
                    case BIGINT:
                        return sourceObject;
                    case FLOAT:
                        return new AFloat(((AInt64) sourceObject).getLongValue());
                    case DOUBLE:
                        return new ADouble(((AInt64) sourceObject).getLongValue());
                    default:
                        throw new AsterixException(
                                "Can't convert the " + sourceTypeTag + " type to the " + targetTypeTag + " type.");
                }
            case FLOAT:
                switch (targetTypeTag) {
                    case TINYINT:
                        // an exception can happen because of a type demotion from FLOAT to TINYINT
                        return new AInt8((byte) ((AFloat) sourceObject).getFloatValue());
                    case SMALLINT:
                        // an exception can happen because of a type demotion from FLOAT to SMALLINT
                        return new AInt16((short) ((AFloat) sourceObject).getFloatValue());
                    case INTEGER:
                        // an exception can happen because of a type demotion from FLOAT to INTEGER
                        return new AInt32((int) ((AFloat) sourceObject).getFloatValue());
                    case BIGINT:
                        // an exception can happen because of a type demotion from FLOAT to BIGINT
                        return new AInt64((long) ((AFloat) sourceObject).getFloatValue());
                    case FLOAT:
                        return sourceObject;
                    case DOUBLE:
                        return new ADouble(((AFloat) sourceObject).getFloatValue());
                    default:
                        throw new AsterixException(
                                "Can't convert the " + sourceTypeTag + " type to the " + targetTypeTag + " type.");
                }
            case DOUBLE:
                switch (targetTypeTag) {
                    case TINYINT:
                        // an exception can happen because of a type demotion from DOUBLE to TINYINT
                        return new AInt8((byte) ((ADouble) sourceObject).getDoubleValue());
                    case SMALLINT:
                        // an exception can happen because of a type demotion from DOUBLE to SMALLINT
                        return new AInt16((short) ((ADouble) sourceObject).getDoubleValue());
                    case INTEGER:
                        // an exception can happen because of a type demotion from DOUBLE to INTEGER
                        return new AInt32((int) ((ADouble) sourceObject).getDoubleValue());
                    case BIGINT:
                        // an exception can happen because of a type demotion from DOUBLE to BIGINT
                        return new AInt64((long) ((ADouble) sourceObject).getDoubleValue());
                    case FLOAT:
                        // an exception can happen because of a type demotion from DOUBLE to FLOAT
                        return new AFloat((float) ((ADouble) sourceObject).getDoubleValue());
                    case DOUBLE:
                        return sourceObject;
                    default:
                        throw new AsterixException(
                                "Can't convert the " + sourceTypeTag + " type to the " + targetTypeTag + " type.");
                }
            default:
                throw new AsterixException("Source type is not a numeric type.");

        }

    }

    // convert a numeric value in a byte array to the target type value
    public static void convertNumericTypeByteArray(byte[] sourceByteArray, int s1, int l1, ATypeTag targetTypeTag,
            DataOutput out) throws AsterixException, IOException {
        ATypeTag sourceTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(sourceByteArray[s1]);

        if (sourceTypeTag != targetTypeTag) {
            // source tag can be promoted to target tag (e.g. tag1: SMALLINT, tag2: INTEGER)
            if (ATypeHierarchy.canPromote(sourceTypeTag, targetTypeTag)) {
                convertComputer = ATypeHierarchy.getTypePromoteComputer(sourceTypeTag, targetTypeTag);;
                convertComputer.convertType(sourceByteArray, s1 + 1, l1 - 1, out);
                // source tag can be demoted to target tag
            } else if (ATypeHierarchy.canDemote(sourceTypeTag, targetTypeTag)) {
                convertComputer = ATypeHierarchy.getTypeDemoteComputer(sourceTypeTag, targetTypeTag);;
                convertComputer.convertType(sourceByteArray, s1 + 1, l1 - 1, out);
            } else {
                throw new IOException(
                        "Can't convert the " + sourceTypeTag + " type to the " + targetTypeTag + " type.");
            }
        }

    }


    // Get an INT value from numeric types array. We assume the first byte contains the type tag.
    public static int getIntegerValue(String name, int argIndex, byte[] bytes, int offset) throws HyracksDataException {
        return getIntegerValueWithDifferentTypeTagPosition(name, argIndex, bytes, offset + 1, offset);
    }

    // Get an INT value from numeric types array. We assume the specific location of a byte array contains the type tag.
    public static int getIntegerValueWithDifferentTypeTagPosition(String name, int argIndex, byte[] bytes, int offset,
            int typeTagPosition) throws HyracksDataException {
        int value;
        ATypeTag sourceTypeTag = ATypeTag.VALUE_TYPE_MAPPING[bytes[typeTagPosition]];

        if (sourceTypeTag == null) {
            throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, name, argIndex);
        }
        switch (sourceTypeTag) {
            case BIGINT:
                value = (int) LongPointable.getLong(bytes, offset);
                break;
            case INTEGER:
                value = IntegerPointable.getInteger(bytes, offset);
                break;
            case TINYINT:
                value = bytes[offset];
                break;
            case SMALLINT:
                value = ShortPointable.getShort(bytes, offset);
                break;
            case FLOAT:
                value = (int) FloatPointable.getFloat(bytes, offset);
                break;
            case DOUBLE:
                value = (int) DoublePointable.getDouble(bytes, offset);
                break;
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_MISMATCH, name, argIndex, sourceTypeTag,
                        ATypeTag.TINYINT, ATypeTag.SMALLINT, ATypeTag.INTEGER, ATypeTag.BIGINT, ATypeTag.FLOAT,
                        ATypeTag.DOUBLE);

        }

        return value;
    }

    // Get a LONG (bigint) value from numeric types array. We assume the first byte contains the type tag.
    public static long getLongValue(String name, int argIndex, byte[] bytes, int offset) throws HyracksDataException {
        return getLongValueWithDifferentTypeTagPosition(name, argIndex, bytes, offset + 1, offset);
    }

    // Get a LONG (bigint) value from numeric types array. We assume the specific location of a byte array
    // contains the type tag.
    private static long getLongValueWithDifferentTypeTagPosition(String name, int argIndex, byte[] bytes, int offset,
            int typeTagPosition) throws HyracksDataException {
        long value;
        ATypeTag sourceTypeTag = ATypeTag.VALUE_TYPE_MAPPING[bytes[typeTagPosition]];
        if (sourceTypeTag == null) {
            throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, name, argIndex);
        }
        switch (sourceTypeTag) {
            case BIGINT:
                value = LongPointable.getLong(bytes, offset);
                break;
            case INTEGER:
                value = IntegerPointable.getInteger(bytes, offset);
                break;
            case TINYINT:
                value = bytes[offset];
                break;
            case SMALLINT:
                value = ShortPointable.getShort(bytes, offset);
                break;
            case FLOAT:
                value = (long) FloatPointable.getFloat(bytes, offset);
                break;
            case DOUBLE:
                value = (long) DoublePointable.getDouble(bytes, offset);
                break;
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_MISMATCH, name, argIndex, sourceTypeTag,
                        ATypeTag.TINYINT, ATypeTag.SMALLINT, ATypeTag.INTEGER, ATypeTag.BIGINT, ATypeTag.FLOAT,
                        ATypeTag.DOUBLE);
        }

        return value;
    }

    // Get a DOUBLE value from numeric types array. We assume the first byte contains the type tag.
    public static double getDoubleValue(String name, int argIndex, byte[] bytes, int offset)
            throws HyracksDataException {
        return getDoubleValueWithDifferentTypeTagPosition(name, argIndex, bytes, offset + 1, offset);
    }

    // Get a DOUBLE value from numeric types array. We assume the specific location of a byte array contains the type tag.
    private static double getDoubleValueWithDifferentTypeTagPosition(String name, int argIndex, byte[] bytes,
            int offset, int typeTagPosition)
            throws HyracksDataException {
        double value;
        ATypeTag sourceTypeTag = ATypeTag.VALUE_TYPE_MAPPING[bytes[typeTagPosition]];
        if (sourceTypeTag == null) {
            throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, name, argIndex);
        }
        switch (sourceTypeTag) {
            case BIGINT:
                value = LongPointable.getLong(bytes, offset);
                break;
            case INTEGER:
                value = IntegerPointable.getInteger(bytes, offset);
                break;
            case TINYINT:
                value = bytes[offset];
                break;
            case SMALLINT:
                value = ShortPointable.getShort(bytes, offset);
                break;
            case FLOAT:
                value = FloatPointable.getFloat(bytes, offset);
                break;
            case DOUBLE:
                value = DoublePointable.getDouble(bytes, offset);
                break;
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_MISMATCH, name, argIndex, sourceTypeTag,
                        ATypeTag.TINYINT, ATypeTag.SMALLINT, ATypeTag.INTEGER, ATypeTag.BIGINT, ATypeTag.FLOAT,
                        ATypeTag.DOUBLE);
        }

        return value;
    }

    public static enum Domain {
        SPATIAL,
        NUMERIC,
        LIST,
        ANY
    }

}
