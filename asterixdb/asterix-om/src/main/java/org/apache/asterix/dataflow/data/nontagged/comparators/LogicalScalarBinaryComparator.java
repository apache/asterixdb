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
package org.apache.asterix.dataflow.data.nontagged.comparators;

import static org.apache.asterix.dataflow.data.common.ILogicalBinaryComparator.inequalityUndefined;

import org.apache.asterix.dataflow.data.common.ILogicalBinaryComparator;
import org.apache.asterix.dataflow.data.common.TaggedValueReference;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADayTimeDurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AYearMonthDurationSerializerDeserializer;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

public final class LogicalScalarBinaryComparator implements ILogicalBinaryComparator {

    private final boolean isEquality;
    private static final LogicalScalarBinaryComparator INSTANCE_EQ = new LogicalScalarBinaryComparator(true);
    private static final LogicalScalarBinaryComparator INSTANCE_INEQ = new LogicalScalarBinaryComparator(false);

    private LogicalScalarBinaryComparator(boolean isEquality) {
        this.isEquality = isEquality;
    }

    static LogicalScalarBinaryComparator of(boolean isEquality) {
        return isEquality ? INSTANCE_EQ : INSTANCE_INEQ;
    }

    @Override
    public Result compare(TaggedValueReference left, TaggedValueReference right) throws HyracksDataException {
        ATypeTag leftTag = left.getTag();
        ATypeTag rightTag = right.getTag();
        Result comparisonResult = ComparatorUtil.returnMissingOrNullOrMismatch(leftTag, rightTag);
        if (comparisonResult != null) {
            return comparisonResult;
        }
        if (comparisonUndefined(leftTag, rightTag, isEquality)) {
            return Result.INCOMPARABLE;
        }
        byte[] leftBytes = left.getByteArray();
        byte[] rightBytes = right.getByteArray();
        int leftStart = left.getStartOffset();
        int rightStart = right.getStartOffset();
        // compare number if one of args is number since compatibility has already been checked above
        if (ATypeHierarchy.getTypeDomain(leftTag) == ATypeHierarchy.Domain.NUMERIC) {
            return ILogicalBinaryComparator.asResult(
                    ComparatorUtil.compareNumbers(leftTag, leftBytes, leftStart, rightTag, rightBytes, rightStart));
        }

        // comparing non-numeric
        // throw an exception if !=, the assumption here is only numeric types are compatible with each other
        if (leftTag != rightTag) {
            throw new IllegalStateException("Two different non-numeric tags but they are compatible");
        }

        int leftLen = left.getLength();
        int rightLen = right.getLength();
        int result;
        switch (leftTag) {
            case BOOLEAN:
                result = BooleanPointable.compare(leftBytes, leftStart, leftLen, rightBytes, rightStart, rightLen);
                break;
            case STRING:
                result = UTF8StringPointable.compare(leftBytes, leftStart, leftLen, rightBytes, rightStart, rightLen);
                break;
            case YEARMONTHDURATION:
                result = Integer.compare(AYearMonthDurationSerializerDeserializer.getYearMonth(leftBytes, leftStart),
                        AYearMonthDurationSerializerDeserializer.getYearMonth(rightBytes, rightStart));
                break;
            case TIME:
                result = Integer.compare(ATimeSerializerDeserializer.getChronon(leftBytes, leftStart),
                        ATimeSerializerDeserializer.getChronon(rightBytes, rightStart));
                break;
            case DATE:
                result = Integer.compare(ADateSerializerDeserializer.getChronon(leftBytes, leftStart),
                        ADateSerializerDeserializer.getChronon(rightBytes, rightStart));
                break;
            case DAYTIMEDURATION:
                result = Long.compare(ADayTimeDurationSerializerDeserializer.getDayTime(leftBytes, leftStart),
                        ADayTimeDurationSerializerDeserializer.getDayTime(rightBytes, rightStart));
                break;
            case DATETIME:
                result = Long.compare(ADateTimeSerializerDeserializer.getChronon(leftBytes, leftStart),
                        ADateTimeSerializerDeserializer.getChronon(rightBytes, rightStart));
                break;
            case CIRCLE:
                result = ACirclePartialBinaryComparatorFactory.compare(leftBytes, leftStart, leftLen, rightBytes,
                        rightStart, rightLen);
                break;
            case LINE:
                result = ALinePartialBinaryComparatorFactory.compare(leftBytes, leftStart, leftLen, rightBytes,
                        rightStart, rightLen);
                break;
            case POINT:
                result = APointPartialBinaryComparatorFactory.compare(leftBytes, leftStart, leftLen, rightBytes,
                        rightStart, rightLen);
                break;
            case POINT3D:
                result = APoint3DPartialBinaryComparatorFactory.compare(leftBytes, leftStart, leftLen, rightBytes,
                        rightStart, rightLen);
                break;
            case POLYGON:
                result = APolygonPartialBinaryComparatorFactory.compare(leftBytes, leftStart, leftLen, rightBytes,
                        rightStart, rightLen);
                break;
            case DURATION:
                result = ADurationPartialBinaryComparatorFactory.compare(leftBytes, leftStart, leftLen, rightBytes,
                        rightStart, rightLen);
                break;
            case INTERVAL:
                result = AIntervalAscPartialBinaryComparatorFactory.compare(leftBytes, leftStart, leftLen, rightBytes,
                        rightStart, rightLen);
                break;
            case RECTANGLE:
                result = ARectanglePartialBinaryComparatorFactory.compare(leftBytes, leftStart, leftLen, rightBytes,
                        rightStart, rightLen);
                break;
            case BINARY:
                result = ByteArrayPointable.compare(leftBytes, leftStart, leftLen, rightBytes, rightStart, rightLen);
                break;
            case UUID:
                result = AUUIDPartialBinaryComparatorFactory.compare(leftBytes, leftStart, leftLen, rightBytes,
                        rightStart, rightLen);
                break;
            default:
                return Result.NULL;
        }
        return ILogicalBinaryComparator.asResult(result);
    }

    @Override
    public Result compare(TaggedValueReference left, IAObject rightConstant) {
        // TODO(ali): currently defined for numbers only
        ATypeTag leftTag = left.getTag();
        ATypeTag rightTag = rightConstant.getType().getTypeTag();
        Result comparisonResult = ComparatorUtil.returnMissingOrNullOrMismatch(leftTag, rightTag);
        if (comparisonResult != null) {
            return comparisonResult;
        }
        if (comparisonUndefined(leftTag, rightTag, isEquality)) {
            return Result.NULL;
        }
        if (ATypeHierarchy.getTypeDomain(leftTag) == ATypeHierarchy.Domain.NUMERIC) {
            return ComparatorUtil.compareNumWithConstant(left, rightConstant);
        }
        return Result.NULL;
    }

    @Override
    public Result compare(IAObject leftConstant, TaggedValueReference right) {
        // TODO(ali): currently defined for numbers only
        Result result = compare(right, leftConstant);
        if (result == Result.LT) {
            return Result.GT;
        } else if (result == Result.GT) {
            return Result.LT;
        }
        return result;
    }

    @Override
    public Result compare(IAObject leftConstant, IAObject rightConstant) {
        // TODO(ali): currently defined for numbers only
        ATypeTag leftTag = leftConstant.getType().getTypeTag();
        ATypeTag rightTag = rightConstant.getType().getTypeTag();
        Result comparisonResult = ComparatorUtil.returnMissingOrNullOrMismatch(leftTag, rightTag);
        if (comparisonResult != null) {
            return comparisonResult;
        }
        if (comparisonUndefined(leftTag, rightTag, isEquality)) {
            return Result.NULL;
        }
        if (ATypeHierarchy.getTypeDomain(leftTag) == ATypeHierarchy.Domain.NUMERIC) {
            return ComparatorUtil.compareConstants(leftConstant, rightConstant);
        }
        return Result.NULL;
    }

    private static boolean comparisonUndefined(ATypeTag leftTag, ATypeTag rightTag, boolean isEquality) {
        return !isEquality && (inequalityUndefined(leftTag) || inequalityUndefined(rightTag));
    }
}
