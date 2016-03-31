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
package org.apache.asterix.runtime.evaluators.comparisons;

import java.io.DataOutput;

import org.apache.asterix.dataflow.data.nontagged.comparators.ABinaryComparator;
import org.apache.asterix.dataflow.data.nontagged.comparators.ACirclePartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ADurationPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AIntervalAscPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ALinePartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.APoint3DPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.APointPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.APolygonPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ARectanglePartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AUUIDPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.BinaryComparatorConstant.ComparableResultCode;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractComparisonEvaluator implements IScalarEvaluator {

    protected enum ComparisonResult {
        LESS_THAN,
        EQUAL,
        GREATER_THAN,
        UNKNOWN
    }

    protected ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected DataOutput out = resultStorage.getDataOutput();
    protected IPointable outLeft = new VoidPointable();
    protected IPointable outRight = new VoidPointable();
    protected IScalarEvaluator evalLeft;
    protected IScalarEvaluator evalRight;
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ABoolean> serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ABOOLEAN);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);
    protected IBinaryComparator strBinaryComp = AqlBinaryComparatorFactoryProvider.UTF8STRING_POINTABLE_INSTANCE
            .createBinaryComparator();
    protected IBinaryComparator circleBinaryComp = ACirclePartialBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();
    protected IBinaryComparator durationBinaryComp = ADurationPartialBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();
    protected IBinaryComparator intervalBinaryComp = AIntervalAscPartialBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();
    protected IBinaryComparator lineBinaryComparator = ALinePartialBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();
    protected IBinaryComparator pointBinaryComparator = APointPartialBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();
    protected IBinaryComparator point3DBinaryComparator = APoint3DPartialBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();
    protected IBinaryComparator polygonBinaryComparator = APolygonPartialBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();
    protected IBinaryComparator rectangleBinaryComparator = ARectanglePartialBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();
    protected IBinaryComparator uuidBinaryComparator = AUUIDPartialBinaryComparatorFactory.INSTANCE
            .createBinaryComparator();

    protected final IBinaryComparator byteArrayComparator = new PointableBinaryComparatorFactory(
            ByteArrayPointable.FACTORY).createBinaryComparator();

    public AbstractComparisonEvaluator(IScalarEvaluatorFactory evalLeftFactory,
            IScalarEvaluatorFactory evalRightFactory, IHyracksTaskContext context) throws AlgebricksException {
        this.evalLeft = evalLeftFactory.createScalarEvaluator(context);
        this.evalRight = evalRightFactory.createScalarEvaluator(context);
    }

    protected void evalInputs(IFrameTupleReference tuple) throws AlgebricksException {
        evalLeft.evaluate(tuple, outLeft);
        evalRight.evaluate(tuple, outRight);
    }

    // checks whether we can apply >, >=, <, and <= operations to the given type since
    // these operations can not be defined for certain types.
    protected void checkTotallyOrderable() throws AlgebricksException {
        if (outLeft.getLength() != 0) {
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                    .deserialize(outLeft.getByteArray()[outLeft.getStartOffset()]);
            switch (typeTag) {
                case DURATION:
                case INTERVAL:
                case LINE:
                case POINT:
                case POINT3D:
                case POLYGON:
                case CIRCLE:
                case RECTANGLE:
                    throw new AlgebricksException(
                            "Comparison operations (GT, GE, LT, and LE) for the " + typeTag + " type are not defined.");
                default:
                    return;
            }
        }
    }

    // checks whether two types are comparable
    protected ComparableResultCode comparabilityCheck() {
        // just check TypeTags
        return ABinaryComparator.isComparable(outLeft.getByteArray(), outLeft.getStartOffset(), 1,
                outRight.getByteArray(), outRight.getStartOffset(), 1);
    }

    protected ComparisonResult compareResults() throws AlgebricksException {
        boolean isLeftNull = false;
        boolean isRightNull = false;
        ATypeTag typeTag1 = null;
        ATypeTag typeTag2 = null;

        byte[] leftBytes = outLeft.getByteArray();
        int leftStartOffset = outLeft.getStartOffset();
        byte[] rightBytes = outRight.getByteArray();
        int rightStartOffset = outRight.getStartOffset();

        if (outLeft.getLength() == 0) {
            isLeftNull = true;
        } else {
            typeTag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(leftBytes[leftStartOffset]);
            if (typeTag1 == ATypeTag.NULL) {
                isLeftNull = true;
            }
        }
        if (outRight.getLength() == 0) {
            isRightNull = true;
        } else {
            typeTag2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(rightBytes[rightStartOffset]);
            if (typeTag2 == ATypeTag.NULL) {
                isRightNull = true;
            }
        }

        if (isLeftNull || isRightNull) {
            return ComparisonResult.UNKNOWN;
        }

        switch (typeTag1) {
            case INT8: {
                return compareInt8WithArg(typeTag2);
            }
            case INT16: {
                return compareInt16WithArg(typeTag2);
            }
            case INT32: {
                return compareInt32WithArg(typeTag2);
            }
            case INT64: {
                return compareInt64WithArg(typeTag2);
            }
            case FLOAT: {
                return compareFloatWithArg(typeTag2);
            }
            case DOUBLE: {
                return compareDoubleWithArg(typeTag2);
            }
            case STRING: {
                return compareStringWithArg(typeTag2);
            }
            case BOOLEAN: {
                return compareBooleanWithArg(typeTag2);
            }

            default: {
                return compareStrongTypedWithArg(typeTag1, typeTag2);
            }
        }
    }

    private ComparisonResult compareStrongTypedWithArg(ATypeTag expectedTypeTag, ATypeTag actualTypeTag)
            throws AlgebricksException {
        if (expectedTypeTag != actualTypeTag) {
            throw new AlgebricksException(
                    "Comparison is undefined between " + expectedTypeTag + " and " + actualTypeTag + ".");
        }
        int result = 0;
        byte[] leftBytes = outLeft.getByteArray();
        int leftOffset = outLeft.getStartOffset() + 1;
        int leftLen = outLeft.getLength() - 1;
        byte[] rightBytes = outRight.getByteArray();
        int rightOffset = outRight.getStartOffset() + 1;
        int rightLen = outRight.getLength() - 1;

        try {
            switch (actualTypeTag) {
                case YEARMONTHDURATION:
                case TIME:
                case DATE:
                    result = Integer.compare(AInt32SerializerDeserializer.getInt(leftBytes, leftOffset),
                            AInt32SerializerDeserializer.getInt(rightBytes, rightOffset));
                    break;
                case DAYTIMEDURATION:
                case DATETIME:
                    result = Long.compare(AInt64SerializerDeserializer.getLong(leftBytes, leftOffset),
                            AInt64SerializerDeserializer.getLong(rightBytes, rightOffset));
                    break;
                case CIRCLE:
                    result = circleBinaryComp.compare(leftBytes, leftOffset, leftLen, rightBytes, rightOffset,
                            rightLen);
                    break;
                case LINE:
                    result = lineBinaryComparator.compare(leftBytes, leftOffset, leftLen, rightBytes, rightOffset,
                            rightLen);
                    break;
                case POINT:
                    result = pointBinaryComparator.compare(leftBytes, leftOffset, leftLen, rightBytes, rightOffset,
                            rightLen);
                    break;
                case POINT3D:
                    result = point3DBinaryComparator.compare(leftBytes, leftOffset, leftLen, rightBytes, rightOffset,
                            rightLen);
                    break;
                case POLYGON:
                    result = polygonBinaryComparator.compare(leftBytes, leftOffset, leftLen, rightBytes, rightOffset,
                            rightLen);
                    break;
                case DURATION:
                    result = durationBinaryComp.compare(leftBytes, leftOffset, leftLen, rightBytes, rightOffset,
                            rightLen);
                    break;
                case INTERVAL:
                    result = intervalBinaryComp.compare(leftBytes, leftOffset, leftLen, rightBytes, rightOffset,
                            rightLen);
                    break;
                case RECTANGLE:
                    result = rectangleBinaryComparator.compare(leftBytes, leftOffset, leftLen, rightBytes, rightOffset,
                            rightLen);
                    break;
                case BINARY:
                    result = byteArrayComparator.compare(leftBytes, leftOffset, leftLen, rightBytes, rightOffset,
                            rightLen);
                    break;
                case UUID:
                    result = uuidBinaryComparator.compare(leftBytes, leftOffset, leftLen, rightBytes, rightOffset,
                            rightLen);
                    break;
                default:
                    throw new AlgebricksException("Comparison for " + actualTypeTag + " is not supported.");
            }
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
        if (result == 0) {
            return ComparisonResult.EQUAL;
        } else if (result < 0) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

    private ComparisonResult compareBooleanWithArg(ATypeTag typeTag2) throws AlgebricksException {
        if (typeTag2 == ATypeTag.BOOLEAN) {
            byte b0 = outLeft.getByteArray()[outLeft.getStartOffset() + 1];
            byte b1 = outRight.getByteArray()[outRight.getStartOffset() + 1];
            return compareByte(b0, b1);
        }
        throw new AlgebricksException("Comparison is undefined between types ABoolean and " + typeTag2 + " .");
    }

    private ComparisonResult compareStringWithArg(ATypeTag typeTag2) throws AlgebricksException {
        if (typeTag2 == ATypeTag.STRING) {
            int result;
            try {
                result = strBinaryComp.compare(outLeft.getByteArray(), outLeft.getStartOffset() + 1,
                        outLeft.getLength() - 1, outRight.getByteArray(), outRight.getStartOffset() + 1,
                        outRight.getLength() - 1);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
            if (result == 0) {
                return ComparisonResult.EQUAL;
            } else if (result < 0) {
                return ComparisonResult.LESS_THAN;
            } else {
                return ComparisonResult.GREATER_THAN;
            }
        }
        throw new AlgebricksException("Comparison is undefined between types AString and " + typeTag2 + " .");
    }

    private ComparisonResult compareDoubleWithArg(ATypeTag typeTag2) throws AlgebricksException {
        byte[] leftBytes = outLeft.getByteArray();
        int leftOffset = outLeft.getStartOffset() + 1;
        byte[] rightBytes = outRight.getByteArray();
        int rightOffset = outRight.getStartOffset() + 1;

        double s = ADoubleSerializerDeserializer.getDouble(leftBytes, leftOffset);
        switch (typeTag2) {
            case INT8: {
                byte v2 = AInt8SerializerDeserializer.getByte(rightBytes, rightOffset);
                return compareDouble(s, v2);
            }
            case INT16: {
                short v2 = AInt16SerializerDeserializer.getShort(rightBytes, rightOffset);
                return compareDouble(s, v2);
            }
            case INT32: {
                int v2 = AInt32SerializerDeserializer.getInt(rightBytes, rightOffset);
                return compareDouble(s, v2);
            }
            case INT64: {
                long v2 = AInt64SerializerDeserializer.getLong(rightBytes, rightOffset);
                return compareDouble(s, v2);
            }
            case FLOAT: {
                float v2 = AFloatSerializerDeserializer.getFloat(rightBytes, rightOffset);
                return compareDouble(s, v2);
            }
            case DOUBLE: {
                double v2 = ADoubleSerializerDeserializer.getDouble(rightBytes, rightOffset);
                return compareDouble(s, v2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types ADouble and " + typeTag2 + " .");
            }
        }
    }

    private ComparisonResult compareFloatWithArg(ATypeTag typeTag2) throws AlgebricksException {
        byte[] leftBytes = outLeft.getByteArray();
        int leftOffset = outLeft.getStartOffset() + 1;
        byte[] rightBytes = outRight.getByteArray();
        int rightOffset = outRight.getStartOffset() + 1;

        float s = FloatPointable.getFloat(leftBytes, leftOffset);
        switch (typeTag2) {
            case INT8: {
                byte v2 = AInt8SerializerDeserializer.getByte(rightBytes, rightOffset);
                return compareFloat(s, v2);
            }
            case INT16: {
                short v2 = AInt16SerializerDeserializer.getShort(rightBytes, rightOffset);
                return compareFloat(s, v2);
            }
            case INT32: {
                int v2 = AInt32SerializerDeserializer.getInt(rightBytes, rightOffset);
                return compareFloat(s, v2);
            }
            case INT64: {
                long v2 = AInt64SerializerDeserializer.getLong(rightBytes, rightOffset);
                return compareFloat(s, v2);
            }
            case FLOAT: {
                float v2 = AFloatSerializerDeserializer.getFloat(rightBytes, rightOffset);
                return compareFloat(s, v2);
            }
            case DOUBLE: {
                double v2 = ADoubleSerializerDeserializer.getDouble(rightBytes, rightOffset);
                return compareDouble(s, v2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types AFloat and " + typeTag2 + " .");
            }
        }
    }

    private ComparisonResult compareInt64WithArg(ATypeTag typeTag2) throws AlgebricksException {
        byte[] leftBytes = outLeft.getByteArray();
        int leftOffset = outLeft.getStartOffset() + 1;
        byte[] rightBytes = outRight.getByteArray();
        int rightOffset = outRight.getStartOffset() + 1;

        long s = AInt64SerializerDeserializer.getLong(leftBytes, leftOffset);
        switch (typeTag2) {
            case INT8: {
                byte v2 = AInt8SerializerDeserializer.getByte(rightBytes, rightOffset);
                return compareLong(s, v2);
            }
            case INT16: {
                short v2 = AInt16SerializerDeserializer.getShort(rightBytes, rightOffset);
                return compareLong(s, v2);
            }
            case INT32: {
                int v2 = AInt32SerializerDeserializer.getInt(rightBytes, rightOffset);
                return compareLong(s, v2);
            }
            case INT64: {
                long v2 = AInt64SerializerDeserializer.getLong(rightBytes, rightOffset);
                return compareLong(s, v2);
            }
            case FLOAT: {
                float v2 = AFloatSerializerDeserializer.getFloat(rightBytes, rightOffset);
                return compareFloat(s, v2);
            }
            case DOUBLE: {
                double v2 = ADoubleSerializerDeserializer.getDouble(rightBytes, rightOffset);
                return compareDouble(s, v2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types AInt64 and " + typeTag2 + " .");
            }
        }
    }

    private ComparisonResult compareInt32WithArg(ATypeTag typeTag2) throws AlgebricksException {
        byte[] leftBytes = outLeft.getByteArray();
        int leftOffset = outLeft.getStartOffset() + 1;
        byte[] rightBytes = outRight.getByteArray();
        int rightOffset = outRight.getStartOffset() + 1;

        int s = IntegerPointable.getInteger(leftBytes, leftOffset);
        switch (typeTag2) {
            case INT8: {
                byte v2 = AInt8SerializerDeserializer.getByte(rightBytes, rightOffset);
                return compareInt(s, v2);
            }
            case INT16: {
                short v2 = AInt16SerializerDeserializer.getShort(rightBytes, rightOffset);
                return compareInt(s, v2);
            }
            case INT32: {
                int v2 = AInt32SerializerDeserializer.getInt(rightBytes, rightOffset);
                return compareInt(s, v2);
            }
            case INT64: {
                long v2 = AInt64SerializerDeserializer.getLong(rightBytes, rightOffset);
                return compareLong(s, v2);
            }
            case FLOAT: {
                float v2 = AFloatSerializerDeserializer.getFloat(rightBytes, rightOffset);
                return compareFloat(s, v2);
            }
            case DOUBLE: {
                double v2 = ADoubleSerializerDeserializer.getDouble(rightBytes, rightOffset);
                return compareDouble(s, v2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types AInt32 and " + typeTag2 + " .");
            }
        }
    }

    private ComparisonResult compareInt16WithArg(ATypeTag typeTag2) throws AlgebricksException {
        byte[] leftBytes = outLeft.getByteArray();
        int leftOffset = outLeft.getStartOffset() + 1;
        byte[] rightBytes = outRight.getByteArray();
        int rightOffset = outRight.getStartOffset() + 1;

        short s = AInt16SerializerDeserializer.getShort(leftBytes, leftOffset);
        switch (typeTag2) {
            case INT8: {
                byte v2 = AInt8SerializerDeserializer.getByte(rightBytes, rightOffset);
                return compareShort(s, v2);
            }
            case INT16: {
                short v2 = AInt16SerializerDeserializer.getShort(rightBytes, rightOffset);
                return compareShort(s, v2);
            }
            case INT32: {
                int v2 = AInt32SerializerDeserializer.getInt(rightBytes, rightOffset);
                return compareInt(s, v2);
            }
            case INT64: {
                long v2 = AInt64SerializerDeserializer.getLong(rightBytes, rightOffset);
                return compareLong(s, v2);
            }
            case FLOAT: {
                float v2 = AFloatSerializerDeserializer.getFloat(rightBytes, rightOffset);
                return compareFloat(s, v2);
            }
            case DOUBLE: {
                double v2 = ADoubleSerializerDeserializer.getDouble(rightBytes, rightOffset);
                return compareDouble(s, v2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types AInt16 and " + typeTag2 + " .");
            }
        }
    }

    private ComparisonResult compareInt8WithArg(ATypeTag typeTag2) throws AlgebricksException {
        byte[] leftBytes = outLeft.getByteArray();
        int leftStart = outLeft.getStartOffset() + 1;
        byte[] rightBytes = outRight.getByteArray();
        int rightStart = outRight.getStartOffset() + 1;

        byte s = AInt8SerializerDeserializer.getByte(leftBytes, leftStart);
        switch (typeTag2) {
            case INT8: {
                byte v2 = AInt8SerializerDeserializer.getByte(rightBytes, rightStart);
                return compareByte(s, v2);
            }
            case INT16: {
                short v2 = AInt16SerializerDeserializer.getShort(rightBytes, rightStart);
                return compareShort(s, v2);
            }
            case INT32: {
                int v2 = AInt32SerializerDeserializer.getInt(rightBytes, rightStart);
                return compareInt(s, v2);
            }
            case INT64: {
                long v2 = AInt64SerializerDeserializer.getLong(rightBytes, rightStart);
                return compareLong(s, v2);
            }
            case FLOAT: {
                float v2 = AFloatSerializerDeserializer.getFloat(rightBytes, rightStart);
                return compareFloat(s, v2);
            }
            case DOUBLE: {
                double v2 = ADoubleSerializerDeserializer.getDouble(rightBytes, rightStart);
                return compareDouble(s, v2);
            }
            default: {
                throw new AlgebricksException("Comparison is undefined between types AInt16 and " + typeTag2 + " .");
            }
        }
    }

    private final ComparisonResult compareByte(int v1, int v2) {
        if (v1 == v2) {
            return ComparisonResult.EQUAL;
        } else if (v1 < v2) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

    private final ComparisonResult compareShort(int v1, int v2) {
        if (v1 == v2) {
            return ComparisonResult.EQUAL;
        } else if (v1 < v2) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

    private final ComparisonResult compareInt(int v1, int v2) {
        if (v1 == v2) {
            return ComparisonResult.EQUAL;
        } else if (v1 < v2) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

    private final ComparisonResult compareLong(long v1, long v2) {
        if (v1 == v2) {
            return ComparisonResult.EQUAL;
        } else if (v1 < v2) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

    private final ComparisonResult compareFloat(float v1, float v2) {
        if (v1 == v2) {
            return ComparisonResult.EQUAL;
        } else if (v1 < v2) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

    private final ComparisonResult compareDouble(double v1, double v2) {
        if (v1 == v2) {
            return ComparisonResult.EQUAL;
        } else if (v1 < v2) {
            return ComparisonResult.LESS_THAN;
        } else {
            return ComparisonResult.GREATER_THAN;
        }
    }

}
