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

    public static enum Domain {
        SPATIAL,
        NUMERIC,
        LIST,
        ANY
    }

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
        // Promotion (widening): INT8 -> INT16 -> INT32 -> INT64 -> FLOAT -> DOUBLE
        // No precision and range loss
        addPromotionRule(ATypeTag.INT8, ATypeTag.INT16, IntegerToInt16TypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT8, ATypeTag.INT32, IntegerToInt32TypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT8, ATypeTag.INT64, IntegerToInt64TypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT16, ATypeTag.INT32, IntegerToInt32TypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT16, ATypeTag.INT64, IntegerToInt64TypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT32, ATypeTag.INT64, IntegerToInt64TypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT8, ATypeTag.DOUBLE, IntegerToDoubleTypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT16, ATypeTag.DOUBLE, IntegerToDoubleTypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT32, ATypeTag.DOUBLE, IntegerToDoubleTypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT64, ATypeTag.DOUBLE, IntegerToDoubleTypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.FLOAT, ATypeTag.DOUBLE, FloatToDoubleTypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT8, ATypeTag.FLOAT, IntegerToFloatTypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT16, ATypeTag.FLOAT, IntegerToFloatTypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT32, ATypeTag.FLOAT, IntegerToFloatTypeConvertComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT64, ATypeTag.FLOAT, IntegerToFloatTypeConvertComputer.INSTANCE);
    }

    static {
        // Demotion (narrowing): DOUBLE -> FLOAT -> INT64 -> INT32 -> INT16 -> INT8
        // Possible precision loss (e.g., FLOAT to INT)
        // This may produce an exception (if source value is greater than target.MAX or less than target.MIN)
        addDemotionRule(ATypeTag.INT16, ATypeTag.INT8, IntegerToInt8TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.INT32, ATypeTag.INT8, IntegerToInt8TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.INT64, ATypeTag.INT8, IntegerToInt8TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.INT32, ATypeTag.INT16, IntegerToInt16TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.INT64, ATypeTag.INT16, IntegerToInt16TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.INT64, ATypeTag.INT32, IntegerToInt32TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.FLOAT, ATypeTag.INT8, FloatToInt8TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.FLOAT, ATypeTag.INT16, FloatToInt16TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.FLOAT, ATypeTag.INT32, FloatToInt32TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.FLOAT, ATypeTag.INT64, FloatToInt64TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.DOUBLE, ATypeTag.INT8, DoubleToInt8TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.DOUBLE, ATypeTag.INT16, DoubleToInt16TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.DOUBLE, ATypeTag.INT32, DoubleToInt32TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.DOUBLE, ATypeTag.INT64, DoubleToInt64TypeConvertComputer.INSTANCE);
        addDemotionRule(ATypeTag.DOUBLE, ATypeTag.FLOAT, DoubleToFloatTypeConvertComputer.INSTANCE);
    }

    static {
        hierarchyDomains.put(ATypeTag.POINT, Domain.SPATIAL);
        hierarchyDomains.put(ATypeTag.LINE, Domain.SPATIAL);
        hierarchyDomains.put(ATypeTag.CIRCLE, Domain.SPATIAL);
        hierarchyDomains.put(ATypeTag.POLYGON, Domain.SPATIAL);
        hierarchyDomains.put(ATypeTag.RECTANGLE, Domain.SPATIAL);
        hierarchyDomains.put(ATypeTag.INT8, Domain.NUMERIC);
        hierarchyDomains.put(ATypeTag.INT16, Domain.NUMERIC);
        hierarchyDomains.put(ATypeTag.INT32, Domain.NUMERIC);
        hierarchyDomains.put(ATypeTag.INT64, Domain.NUMERIC);
        hierarchyDomains.put(ATypeTag.FLOAT, Domain.NUMERIC);
        hierarchyDomains.put(ATypeTag.DOUBLE, Domain.NUMERIC);
        hierarchyDomains.put(ATypeTag.ORDEREDLIST, Domain.LIST);
        hierarchyDomains.put(ATypeTag.UNORDEREDLIST, Domain.LIST);
    }

    public static boolean isSameTypeDomain(ATypeTag tag1, ATypeTag tag2, boolean useListDomain) {
        Domain tagHierarchy1 = hierarchyDomains.get(tag1);
        Domain tagHierarchy2 = hierarchyDomains.get(tag2);
        if (tagHierarchy1 == null || tagHierarchy2 == null)
            return false;
        if (useListDomain && tagHierarchy1 == Domain.LIST && tagHierarchy2 == Domain.LIST)
            return true;
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
        return canPromote(type1, type2) | canPromote(type2, type1);
    }

    public static boolean isDemoteCompatible(ATypeTag type1, ATypeTag type2) {
        return canDemote(type1, type2) | canDemote(type2, type1);
    }

    // Get an AsterixConstantValue from a source Object
    public static AsterixConstantValue getAsterixConstantValueFromNumericTypeObject(IAObject sourceObject,
            ATypeTag targetTypeTag) throws AlgebricksException {
        ATypeTag sourceTypeTag = sourceObject.getType().getTypeTag();
        AsterixConstantValue asterixNewConstantValue = null;
        short tmpShortValue = 0;
        int tmpIntValue = 0;
        long tmpLongValue = 0;
        float tmpFloatValue = 0.0f;
        double tmpDoubleValue = 0.0;

        // if the constant type and target type does not match, we do a type conversion
        if (sourceTypeTag != targetTypeTag) {

            switch (targetTypeTag) {
            //Target Field Type:INT64
                case INT64:

                    // Change the Constant Type to INT64 Type
                    switch (sourceTypeTag) {
                        case INT8:
                            asterixNewConstantValue = new AsterixConstantValue(new AInt64(
                                    (long) ((AInt8) sourceObject).getByteValue()));
                            break;

                        case INT16:
                            asterixNewConstantValue = new AsterixConstantValue(new AInt64(
                                    (long) ((AInt16) sourceObject).getShortValue()));
                            break;

                        case INT32:
                            asterixNewConstantValue = new AsterixConstantValue(new AInt64(
                                    (long) ((AInt32) sourceObject).getIntegerValue()));
                            break;

                        case FLOAT:
                            tmpFloatValue = ((AFloat) sourceObject).getFloatValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.FLOAT, ATypeTag.INT64, tmpFloatValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt64((long) tmpFloatValue));
                            break;

                        case DOUBLE:
                            tmpDoubleValue = ((ADouble) sourceObject).getDoubleValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.DOUBLE, ATypeTag.INT64, tmpDoubleValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt64((long) tmpDoubleValue));
                            break;

                        default:
                            break;
                    }

                    break;

                //Target Field Type:INT32
                case INT32:

                    // Change the Constant Type to INT32 Type
                    switch (sourceTypeTag) {
                        case INT8:
                            asterixNewConstantValue = new AsterixConstantValue(new AInt32(
                                    (int) ((AInt8) sourceObject).getByteValue()));
                            break;

                        case INT16:
                            asterixNewConstantValue = new AsterixConstantValue(new AInt32(
                                    (int) ((AInt16) sourceObject).getShortValue()));
                            break;

                        case INT64:
                            tmpLongValue = ((AInt64) sourceObject).getLongValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.INT64, ATypeTag.INT32, tmpLongValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt32((int) tmpLongValue));
                            break;

                        case FLOAT:
                            tmpFloatValue = ((AFloat) sourceObject).getFloatValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.FLOAT, ATypeTag.INT32, tmpFloatValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt32((int) tmpFloatValue));
                            break;

                        case DOUBLE:
                            tmpDoubleValue = ((ADouble) sourceObject).getDoubleValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.FLOAT, ATypeTag.INT32, tmpDoubleValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt32((int) tmpDoubleValue));
                            break;

                        default:
                            break;
                    }

                    break;

                //Target Field Type:INT8
                case INT8:

                    // Change the Constant Type to INT8 Type
                    switch (sourceTypeTag) {
                        case INT16:
                            tmpShortValue = ((AInt16) sourceObject).getShortValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.INT16, ATypeTag.INT8, tmpShortValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt8((byte) tmpShortValue));
                            break;

                        case INT32:
                            tmpIntValue = ((AInt32) sourceObject).getIntegerValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.INT32, ATypeTag.INT8, tmpIntValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt8((byte) tmpIntValue));
                            break;

                        case INT64:
                            tmpLongValue = ((AInt64) sourceObject).getLongValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.INT64, ATypeTag.INT8, tmpLongValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt8((byte) tmpLongValue));
                            break;

                        case FLOAT:
                            tmpFloatValue = ((AFloat) sourceObject).getFloatValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.FLOAT, ATypeTag.INT8, tmpFloatValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt8((byte) tmpFloatValue));
                            break;

                        case DOUBLE:
                            tmpDoubleValue = ((ADouble) sourceObject).getDoubleValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.DOUBLE, ATypeTag.INT8, tmpDoubleValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt8((byte) tmpDoubleValue));
                            break;

                        default:
                            break;
                    }
                    break;

                //Target Field Type:INT16
                case INT16:
                    // Change the Constant Type to INT16 Type
                    switch (sourceTypeTag) {
                        case INT8:
                            asterixNewConstantValue = new AsterixConstantValue(new AInt16(
                                    (short) ((AInt8) sourceObject).getByteValue()));
                            break;

                        case INT32:
                            tmpIntValue = ((AInt32) sourceObject).getIntegerValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.INT32, ATypeTag.INT16, tmpIntValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt16((short) tmpIntValue));
                            break;

                        case INT64:
                            tmpLongValue = ((AInt64) sourceObject).getLongValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.INT64, ATypeTag.INT16, tmpLongValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt16((short) tmpLongValue));
                            break;

                        case FLOAT:
                            tmpFloatValue = ((AFloat) sourceObject).getFloatValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.FLOAT, ATypeTag.INT16, tmpFloatValue);
                            asterixNewConstantValue = new AsterixConstantValue(new AInt16((short) tmpFloatValue));
                            break;

                        case DOUBLE:
                            tmpDoubleValue = ((ADouble) sourceObject).getDoubleValue();
                            // Check whether this value is within the range of the field type
                            valueSanitycheck(ATypeTag.DOUBLE, ATypeTag.INT16, tmpDoubleValue);
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
                        case INT8:
                            asterixNewConstantValue = new AsterixConstantValue(new AFloat(
                                    (float) ((AInt8) sourceObject).getByteValue()));
                            break;

                        case INT16:
                            asterixNewConstantValue = new AsterixConstantValue(new AFloat(
                                    (float) ((AInt16) sourceObject).getShortValue()));
                            break;

                        case INT32:
                            asterixNewConstantValue = new AsterixConstantValue(new AFloat(
                                    (float) (int) ((AInt32) sourceObject).getIntegerValue()));
                            break;

                        case INT64:
                            asterixNewConstantValue = new AsterixConstantValue(new AFloat(
                                    (float) ((AInt64) sourceObject).getLongValue()));
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
                        case INT8:
                            asterixNewConstantValue = new AsterixConstantValue(new ADouble(
                                    (double) ((AInt8) sourceObject).getByteValue()));
                            break;

                        case INT16:
                            asterixNewConstantValue = new AsterixConstantValue(new ADouble(
                                    (double) ((AInt16) sourceObject).getShortValue()));
                            break;

                        case INT32:
                            asterixNewConstantValue = new AsterixConstantValue(new ADouble(
                                    (double) (int) ((AInt32) sourceObject).getIntegerValue()));
                            break;

                        case INT64:
                            asterixNewConstantValue = new AsterixConstantValue(new ADouble(
                                    (double) ((AInt64) sourceObject).getLongValue()));
                            break;

                        case FLOAT:
                            asterixNewConstantValue = new AsterixConstantValue(new ADouble(
                                    (double) ((AFloat) sourceObject).getFloatValue()));
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
            case INT8:
                if (sourceValue > Byte.MAX_VALUE || sourceValue < Byte.MIN_VALUE) {
                    canConvert = false;
                }
                break;

            case INT16:
                if (sourceValue > Short.MAX_VALUE || sourceValue < Short.MIN_VALUE) {
                    canConvert = false;
                }
                break;
            case INT32:
                if (sourceValue > Integer.MAX_VALUE || sourceValue < Integer.MIN_VALUE) {
                    canConvert = false;
                }
                break;
            case INT64:
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
            case INT8:
                switch (targetTypeTag) {
                    case INT8:
                        return sourceObject;
                    case INT16:
                        return new AInt16((short) ((AInt8) sourceObject).getByteValue());
                    case INT32:
                        return new AInt32((int) ((AInt8) sourceObject).getByteValue());
                    case INT64:
                        return new AInt64((long) ((AInt8) sourceObject).getByteValue());
                    case FLOAT:
                        return new AFloat((float) ((AInt8) sourceObject).getByteValue());
                    case DOUBLE:
                        return new ADouble((double) ((AInt8) sourceObject).getByteValue());
                    default:
                        throw new AsterixException("Can't convert the " + sourceTypeTag + " type to the "
                                + targetTypeTag + " type.");
                }
            case INT16:
                switch (targetTypeTag) {
                    case INT8:
                        // an exception can happen because of a type demotion from INT16 to INT8
                        return new AInt8((byte) ((AInt16) sourceObject).getShortValue());
                    case INT16:
                        return sourceObject;
                    case INT32:
                        return new AInt32((int) ((AInt16) sourceObject).getShortValue());
                    case INT64:
                        return new AInt64((long) ((AInt16) sourceObject).getShortValue());
                    case FLOAT:
                        return new AFloat((float) ((AInt16) sourceObject).getShortValue());
                    case DOUBLE:
                        return new ADouble((double) ((AInt16) sourceObject).getShortValue());
                    default:
                        throw new AsterixException("Can't convert the " + sourceTypeTag + " type to the "
                                + targetTypeTag + " type.");
                }

            case INT32:
                switch (targetTypeTag) {
                    case INT8:
                        // an exception can happen because of a type demotion from INT32 to INT8
                        return new AInt8((byte) ((AInt32) sourceObject).getIntegerValue().byteValue());
                    case INT16:
                        // an exception can happen because of a type demotion from INT32 to INT16
                        return new AInt16((short) ((AInt32) sourceObject).getIntegerValue().shortValue());
                    case INT32:
                        return sourceObject;
                    case INT64:
                        return new AInt64((long) ((AInt32) sourceObject).getIntegerValue());
                    case FLOAT:
                        return new AFloat((float) ((AInt32) sourceObject).getIntegerValue());
                    case DOUBLE:
                        return new ADouble((double) ((AInt32) sourceObject).getIntegerValue());
                    default:
                        throw new AsterixException("Can't convert the " + sourceTypeTag + " type to the "
                                + targetTypeTag + " type.");
                }

            case INT64:
                switch (targetTypeTag) {
                    case INT8:
                        // an exception can happen because of a type demotion from INT64 to INT8
                        return new AInt8((byte) ((AInt64) sourceObject).getLongValue());
                    case INT16:
                        // an exception can happen because of a type demotion from INT64 to INT16
                        return new AInt16((short) ((AInt64) sourceObject).getLongValue());
                    case INT32:
                        // an exception can happen because of a type demotion from INT64 to INT32
                        return new AInt32((int) ((AInt64) sourceObject).getLongValue());
                    case INT64:
                        return sourceObject;
                    case FLOAT:
                        return new AFloat((float) ((AInt64) sourceObject).getLongValue());
                    case DOUBLE:
                        return new ADouble((double) ((AInt64) sourceObject).getLongValue());
                    default:
                        throw new AsterixException("Can't convert the " + sourceTypeTag + " type to the "
                                + targetTypeTag + " type.");
                }
            case FLOAT:
                switch (targetTypeTag) {
                    case INT8:
                        // an exception can happen because of a type demotion from FLOAT to INT8
                        return new AInt8((byte) ((AFloat) sourceObject).getFloatValue());
                    case INT16:
                        // an exception can happen because of a type demotion from FLOAT to INT16
                        return new AInt16((short) ((AFloat) sourceObject).getFloatValue());
                    case INT32:
                        // an exception can happen because of a type demotion from FLOAT to INT32
                        return new AInt32((int) ((AFloat) sourceObject).getFloatValue());
                    case INT64:
                        // an exception can happen because of a type demotion from FLOAT to INT64
                        return new AInt64((long) ((AFloat) sourceObject).getFloatValue());
                    case FLOAT:
                        return sourceObject;
                    case DOUBLE:
                        return new ADouble((double) ((AFloat) sourceObject).getFloatValue());
                    default:
                        throw new AsterixException("Can't convert the " + sourceTypeTag + " type to the "
                                + targetTypeTag + " type.");
                }
            case DOUBLE:
                switch (targetTypeTag) {
                    case INT8:
                        // an exception can happen because of a type demotion from DOUBLE to INT8
                        return new AInt8((byte) ((ADouble) sourceObject).getDoubleValue());
                    case INT16:
                        // an exception can happen because of a type demotion from DOUBLE to INT16
                        return new AInt16((short) ((ADouble) sourceObject).getDoubleValue());
                    case INT32:
                        // an exception can happen because of a type demotion from DOUBLE to INT32
                        return new AInt32((int) ((ADouble) sourceObject).getDoubleValue());
                    case INT64:
                        // an exception can happen because of a type demotion from DOUBLE to INT64
                        return new AInt64((long) ((ADouble) sourceObject).getDoubleValue());
                    case FLOAT:
                        // an exception can happen because of a type demotion from DOUBLE to FLOAT
                        return new AFloat((float) ((ADouble) sourceObject).getDoubleValue());
                    case DOUBLE:
                        return sourceObject;
                    default:
                        throw new AsterixException("Can't convert the " + sourceTypeTag + " type to the "
                                + targetTypeTag + " type.");
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
            // source tag can be promoted to target tag (e.g. tag1: INT16, tag2: INT32)
            if (ATypeHierarchy.canPromote(sourceTypeTag, targetTypeTag)) {
                convertComputer = ATypeHierarchy.getTypePromoteComputer(sourceTypeTag, targetTypeTag);;
                convertComputer.convertType(sourceByteArray, s1 + 1, l1 - 1, out);
                // source tag can be demoted to target tag
            } else if (ATypeHierarchy.canDemote(sourceTypeTag, targetTypeTag)) {
                convertComputer = ATypeHierarchy.getTypeDemoteComputer(sourceTypeTag, targetTypeTag);;
                convertComputer.convertType(sourceByteArray, s1 + 1, l1 - 1, out);
            } else {
                throw new IOException("Can't convert the " + sourceTypeTag + " type to the " + targetTypeTag + " type.");
            }
        }

    }

    // Get an INT value from numeric types array. We assume the first byte contains the type tag.
    public static int getIntegerValue(byte[] bytes, int offset) throws HyracksDataException {
        return getIntegerValueWithDifferentTypeTagPosition(bytes, offset + 1, offset);
    }

    // Get an INT value from numeric types array. We assume the specific location of a byte array contains the type tag.
    public static int getIntegerValueWithDifferentTypeTagPosition(byte[] bytes, int offset, int typeTagPosition)
            throws HyracksDataException {
        int value = 0;

        ATypeTag sourceTypeTag = ATypeTag.VALUE_TYPE_MAPPING[bytes[typeTagPosition]];

        switch (sourceTypeTag) {
            case INT64:
                value = (int) LongPointable.getLong(bytes, offset);
                break;
            case INT32:
                value = IntegerPointable.getInteger(bytes, offset);
                break;
            case INT8:
                value = (int) bytes[offset];
                break;
            case INT16:
                value = (int) ShortPointable.getShort(bytes, offset);
                break;
            case FLOAT:
                value = (int) FloatPointable.getFloat(bytes, offset);
                break;
            case DOUBLE:
                value = (int) DoublePointable.getDouble(bytes, offset);
                break;
            default:
                throw new HyracksDataException(
                        "Type casting error while getting an INT32 value: expected INT8/16/32/64/FLOAT/DOUBLE but got "
                                + sourceTypeTag + ".");
        }

        return value;
    }

    // Get a LONG (INT64) value from numeric types array. We assume the first byte contains the type tag.
    public static long getLongValue(byte[] bytes, int offset) throws HyracksDataException {
        return getLongValueWithDifferentTypeTagPosition(bytes, offset + 1, offset);
    }

    // Get a LONG (INT64) value from numeric types array. We assume the specific location of a byte array contains the type tag.
    public static long getLongValueWithDifferentTypeTagPosition(byte[] bytes, int offset, int typeTagPosition)
            throws HyracksDataException {
        long value = 0;

        ATypeTag sourceTypeTag = ATypeTag.VALUE_TYPE_MAPPING[bytes[typeTagPosition]];

        switch (sourceTypeTag) {
            case INT64:
                value = LongPointable.getLong(bytes, offset);
                break;
            case INT32:
                value = (long) IntegerPointable.getInteger(bytes, offset);
                break;
            case INT8:
                value = (long) bytes[offset];
                break;
            case INT16:
                value = (long) ShortPointable.getShort(bytes, offset);
                break;
            case FLOAT:
                value = (long) FloatPointable.getFloat(bytes, offset);
                break;
            case DOUBLE:
                value = (long) DoublePointable.getDouble(bytes, offset);
                break;
            default:
                throw new HyracksDataException(
                        "Type casting error while getting an INT64 value: expected INT8/16/32/64/FLOAT/DOUBLE but got "
                                + sourceTypeTag + ".");
        }

        return value;
    }

    // Get a FLOAT value from numeric types array. We assume the first byte contains the type tag.
    public static float getFloatValue(byte[] bytes, int offset) throws HyracksDataException {
        return getFloatValueWithDifferentTypeTagPosition(bytes, offset + 1, offset);
    }

    // Get a FLOAT value from numeric types array. We assume the specific location of a byte array contains the type tag.
    public static float getFloatValueWithDifferentTypeTagPosition(byte[] bytes, int offset, int typeTagPosition)
            throws HyracksDataException {
        float value = 0;

        ATypeTag sourceTypeTag = ATypeTag.VALUE_TYPE_MAPPING[bytes[typeTagPosition]];

        switch (sourceTypeTag) {
            case INT64:
                value = (float) LongPointable.getLong(bytes, offset);
                break;
            case INT32:
                value = (float) IntegerPointable.getInteger(bytes, offset);
                break;
            case INT8:
                value = (float) bytes[offset];
                break;
            case INT16:
                value = (float) ShortPointable.getShort(bytes, offset);
                break;
            case FLOAT:
                value = FloatPointable.getFloat(bytes, offset);
                break;
            case DOUBLE:
                value = (float) DoublePointable.getDouble(bytes, offset);
                break;
            default:
                throw new HyracksDataException(
                        "Type casting error while getting a FLOAT value: expected INT8/16/32/64/FLOAT/DOUBLE but got "
                                + sourceTypeTag + ".");
        }

        return value;
    }

    // Get a DOUBLE value from numeric types array. We assume the first byte contains the type tag.
    public static double getDoubleValue(byte[] bytes, int offset) throws HyracksDataException {
        return getDoubleValueWithDifferentTypeTagPosition(bytes, offset + 1, offset);
    }

    // Get a DOUBLE value from numeric types array. We assume the specific location of a byte array contains the type tag.
    public static double getDoubleValueWithDifferentTypeTagPosition(byte[] bytes, int offset, int typeTagPosition)
            throws HyracksDataException {
        double value = 0;

        ATypeTag sourceTypeTag = ATypeTag.VALUE_TYPE_MAPPING[bytes[typeTagPosition]];

        switch (sourceTypeTag) {
            case INT64:
                value = (double) LongPointable.getLong(bytes, offset);
                break;
            case INT32:
                value = (double) IntegerPointable.getInteger(bytes, offset);
                break;
            case INT8:
                value = (double) bytes[offset];
                break;
            case INT16:
                value = (double) ShortPointable.getShort(bytes, offset);
                break;
            case FLOAT:
                value = (double) FloatPointable.getFloat(bytes, offset);
                break;
            case DOUBLE:
                value = DoublePointable.getDouble(bytes, offset);
                break;
            default:
                throw new HyracksDataException(
                        "Type casting error while getting a DOUBLE value: expected INT8/16/32/64/FLOAT/DOUBLE but got "
                                + sourceTypeTag + ".");
        }

        return value;
    }

}
