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

import java.io.Serializable;

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
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.exceptions.IncompatibleTypeException;
import org.apache.asterix.runtime.exceptions.UnsupportedTypeException;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;

import static org.apache.asterix.om.types.ATypeTag.TINYINT;

public class ComparisonHelper implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String COMPARISON = "comparison operations (>, >=, <, and <=)";

    private final IBinaryComparator strBinaryComp =
            BinaryComparatorFactoryProvider.UTF8STRING_POINTABLE_INSTANCE.createBinaryComparator();
    private final IBinaryComparator circleBinaryComp =
            ACirclePartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator durationBinaryComp =
            ADurationPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator intervalBinaryComp =
            AIntervalAscPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator lineBinaryComparator =
            ALinePartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator pointBinaryComparator =
            APointPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator point3DBinaryComparator =
            APoint3DPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator polygonBinaryComparator =
            APolygonPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator rectangleBinaryComparator =
            ARectanglePartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator uuidBinaryComparator =
            AUUIDPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator byteArrayComparator =
            new PointableBinaryComparatorFactory(ByteArrayPointable.FACTORY).createBinaryComparator();

    private final SourceLocation sourceLoc;

    public ComparisonHelper(SourceLocation sourceLoc) {
        this.sourceLoc = sourceLoc;
    }

    public int compare(ATypeTag typeTag1, ATypeTag typeTag2, IPointable arg1, IPointable arg2, Number obj1, Number obj2)
            throws HyracksDataException {
        switch (typeTag1) {
            case TINYINT:
                return compareInt8WithArg(typeTag2, arg1, arg2, obj1, obj2);
            case SMALLINT:
                return compareInt16WithArg(typeTag2, arg1, arg2, obj1, obj2);
            case INTEGER:
                return compareInt32WithArg(typeTag2, arg1, arg2, obj1, obj2);
            case BIGINT:
                return compareInt64WithArg(typeTag2, arg1, arg2, obj1, obj2);
            case FLOAT:
                return compareFloatWithArg(typeTag2, arg1, arg2, obj1, obj2);
            case DOUBLE:
                return compareDoubleWithArg(typeTag2, arg1, arg2, obj1, obj2);
            case STRING:
                return compareStringWithArg(typeTag2, arg1, arg2);
            case BOOLEAN:
                return compareBooleanWithArg(typeTag2, arg1, arg2);
            default:
                return compareStrongTypedWithArg(typeTag1, typeTag2, arg1, arg2);
        }
    }

    public int compare(ATypeTag typeTag1, ATypeTag typeTag2, IPointable arg1, IPointable arg2)
            throws HyracksDataException {
        return compare(typeTag1, typeTag2, arg1, arg2, null, null);
    }

    private int compareStrongTypedWithArg(ATypeTag expectedTypeTag, ATypeTag actualTypeTag, IPointable arg1,
            IPointable arg2) throws HyracksDataException {
        if (expectedTypeTag != actualTypeTag) {
            throw new IncompatibleTypeException(sourceLoc, COMPARISON, actualTypeTag.serialize(),
                    expectedTypeTag.serialize());
        }
        int result;
        byte[] leftBytes = arg1.getByteArray();
        int leftOffset = arg1.getStartOffset();
        int leftLen = arg1.getLength() - 1;
        byte[] rightBytes = arg2.getByteArray();
        int rightOffset = arg2.getStartOffset();
        int rightLen = arg2.getLength() - 1;

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
                result = circleBinaryComp.compare(leftBytes, leftOffset, leftLen, rightBytes, rightOffset, rightLen);
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
                result = durationBinaryComp.compare(leftBytes, leftOffset, leftLen, rightBytes, rightOffset, rightLen);
                break;
            case INTERVAL:
                result = intervalBinaryComp.compare(leftBytes, leftOffset, leftLen, rightBytes, rightOffset, rightLen);
                break;
            case RECTANGLE:
                result = rectangleBinaryComparator.compare(leftBytes, leftOffset, leftLen, rightBytes, rightOffset,
                        rightLen);
                break;
            case BINARY:
                result = byteArrayComparator.compare(leftBytes, leftOffset, leftLen, rightBytes, rightOffset, rightLen);
                break;
            case UUID:
                result = uuidBinaryComparator.compare(leftBytes, leftOffset, leftLen, rightBytes, rightOffset,
                        rightLen);
                break;
            default:
                throw new UnsupportedTypeException(sourceLoc, COMPARISON, actualTypeTag.serialize());
        }
        return result;
    }

    private int compareBooleanWithArg(ATypeTag typeTag2, IPointable arg1, IPointable arg2) throws HyracksDataException {
        if (typeTag2 == ATypeTag.BOOLEAN) {
            byte b0 = arg1.getByteArray()[arg1.getStartOffset()];
            byte b1 = arg2.getByteArray()[arg2.getStartOffset()];
            return compareByte(b0, b1);
        }
        throw new IncompatibleTypeException(sourceLoc, COMPARISON, ATypeTag.SERIALIZED_BOOLEAN_TYPE_TAG,
                typeTag2.serialize());
    }

    private int compareStringWithArg(ATypeTag typeTag2, IPointable arg1, IPointable arg2) throws HyracksDataException {
        if (typeTag2 == ATypeTag.STRING) {
            return strBinaryComp.compare(arg1.getByteArray(), arg1.getStartOffset(), arg1.getLength() - 1,
                    arg2.getByteArray(), arg2.getStartOffset(), arg2.getLength() - 1);
        }
        throw new IncompatibleTypeException(sourceLoc, COMPARISON, ATypeTag.SERIALIZED_STRING_TYPE_TAG,
                typeTag2.serialize());
    }

    private int compareDoubleWithArg(ATypeTag typeTag2, IPointable arg1, IPointable arg2, Number obj1, Number obj2)
            throws HyracksDataException {
        byte[] leftBytes = arg1.getByteArray();
        int leftOffset = arg1.getStartOffset();
        byte[] rightBytes = arg2.getByteArray();
        int rightOffset = arg2.getStartOffset();

        double s = getOrDeserializeDouble(leftBytes, leftOffset, obj1);
        switch (typeTag2) {
            case TINYINT:
                return compareDouble(s, getOrDeserializeTinyInt(rightBytes, rightOffset, obj2));
            case SMALLINT:
                return compareDouble(s, getOrDeserializeSmallInt(rightBytes, rightOffset, obj2));
            case INTEGER:
                return compareDouble(s, getOrDeserializeInt(rightBytes, rightOffset, obj2));
            case BIGINT:
                return compareDouble(s, getOrDeserializeBigInt(rightBytes, rightOffset, obj2));
            case FLOAT:
                return compareDouble(s, getOrDeserializeFloat(rightBytes, rightOffset, obj2));
            case DOUBLE:
                return compareDouble(s, getOrDeserializeDouble(rightBytes, rightOffset, obj2));
            default: {
                throw new IncompatibleTypeException(sourceLoc, COMPARISON, ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG,
                        typeTag2.serialize());
            }
        }
    }

    private int compareFloatWithArg(ATypeTag typeTag2, IPointable arg1, IPointable arg2, Number obj1, Number obj2)
            throws HyracksDataException {
        byte[] leftBytes = arg1.getByteArray();
        int leftOffset = arg1.getStartOffset();
        byte[] rightBytes = arg2.getByteArray();
        int rightOffset = arg2.getStartOffset();

        float s = getOrDeserializeFloat(leftBytes, leftOffset, obj1);
        switch (typeTag2) {
            case TINYINT:
                return compareFloat(s, getOrDeserializeTinyInt(rightBytes, rightOffset, obj2));
            case SMALLINT:
                return compareFloat(s, getOrDeserializeSmallInt(rightBytes, rightOffset, obj2));
            case INTEGER:
                return compareFloat(s, getOrDeserializeInt(rightBytes, rightOffset, obj2));
            case BIGINT:
                return compareFloat(s, getOrDeserializeBigInt(rightBytes, rightOffset, obj2));
            case FLOAT:
                return compareFloat(s, getOrDeserializeFloat(rightBytes, rightOffset, obj2));
            case DOUBLE:
                return compareDouble(s, getOrDeserializeDouble(rightBytes, rightOffset, obj2));
            default:
                throw new IncompatibleTypeException(sourceLoc, COMPARISON, ATypeTag.SERIALIZED_FLOAT_TYPE_TAG,
                        typeTag2.serialize());
        }
    }

    private int compareInt64WithArg(ATypeTag typeTag2, IPointable arg1, IPointable arg2, Number obj1, Number obj2)
            throws HyracksDataException {
        byte[] leftBytes = arg1.getByteArray();
        int leftOffset = arg1.getStartOffset();
        byte[] rightBytes = arg2.getByteArray();
        int rightOffset = arg2.getStartOffset();

        long s = getOrDeserializeBigInt(leftBytes, leftOffset, obj1);
        switch (typeTag2) {
            case TINYINT:
                return compareLong(s, getOrDeserializeTinyInt(rightBytes, rightOffset, obj2));
            case SMALLINT:
                return compareLong(s, getOrDeserializeSmallInt(rightBytes, rightOffset, obj2));
            case INTEGER:
                return compareLong(s, getOrDeserializeInt(rightBytes, rightOffset, obj2));
            case BIGINT:
                return compareLong(s, getOrDeserializeBigInt(rightBytes, rightOffset, obj2));
            case FLOAT:
                return compareFloat(s, getOrDeserializeFloat(rightBytes, rightOffset, obj2));
            case DOUBLE:
                return compareDouble(s, getOrDeserializeDouble(rightBytes, rightOffset, obj2));
            default:
                throw new IncompatibleTypeException(sourceLoc, COMPARISON, ATypeTag.SERIALIZED_INT64_TYPE_TAG,
                        typeTag2.serialize());
        }
    }

    private int compareInt32WithArg(ATypeTag typeTag2, IPointable arg1, IPointable arg2, Number obj1, Number obj2)
            throws HyracksDataException {
        byte[] leftBytes = arg1.getByteArray();
        int leftOffset = arg1.getStartOffset();
        byte[] rightBytes = arg2.getByteArray();
        int rightOffset = arg2.getStartOffset();

        int s = getOrDeserializeInt(leftBytes, leftOffset, obj1);
        switch (typeTag2) {
            case TINYINT:
                return compareInt(s, getOrDeserializeTinyInt(rightBytes, rightOffset, obj2));
            case SMALLINT:
                return compareInt(s, getOrDeserializeSmallInt(rightBytes, rightOffset, obj2));
            case INTEGER:
                return compareInt(s, getOrDeserializeInt(rightBytes, rightOffset, obj2));
            case BIGINT:
                return compareLong(s, getOrDeserializeBigInt(rightBytes, rightOffset, obj2));
            case FLOAT:
                return compareFloat(s, getOrDeserializeFloat(rightBytes, rightOffset, obj2));
            case DOUBLE:
                return compareDouble(s, getOrDeserializeDouble(rightBytes, rightOffset, obj2));
            default:
                throw new IncompatibleTypeException(sourceLoc, COMPARISON, ATypeTag.SERIALIZED_INT32_TYPE_TAG,
                        typeTag2.serialize());
        }
    }

    private int compareInt16WithArg(ATypeTag typeTag2, IPointable arg1, IPointable arg2, Number obj1, Number obj2)
            throws HyracksDataException {
        byte[] leftBytes = arg1.getByteArray();
        int leftOffset = arg1.getStartOffset();
        byte[] rightBytes = arg2.getByteArray();
        int rightOffset = arg2.getStartOffset();

        short s = getOrDeserializeSmallInt(leftBytes, leftOffset, obj1);
        switch (typeTag2) {
            case TINYINT:
                return compareShort(s, getOrDeserializeTinyInt(rightBytes, rightOffset, obj2));
            case SMALLINT:
                return compareShort(s, getOrDeserializeSmallInt(rightBytes, rightOffset, obj2));
            case INTEGER:
                return compareInt(s, getOrDeserializeInt(rightBytes, rightOffset, obj2));
            case BIGINT:
                return compareLong(s, getOrDeserializeBigInt(rightBytes, rightOffset, obj2));
            case FLOAT:
                return compareFloat(s, getOrDeserializeFloat(rightBytes, rightOffset, obj2));
            case DOUBLE:
                return compareDouble(s, getOrDeserializeDouble(rightBytes, rightOffset, obj2));
            default:
                throw new IncompatibleTypeException(sourceLoc, COMPARISON, ATypeTag.SERIALIZED_INT16_TYPE_TAG,
                        typeTag2.serialize());
        }
    }

    private int compareInt8WithArg(ATypeTag typeTag2, IPointable arg1, IPointable arg2, Number obj1, Number obj2)
            throws HyracksDataException {
        byte[] leftBytes = arg1.getByteArray();
        int leftStart = arg1.getStartOffset();
        byte[] rightBytes = arg2.getByteArray();
        int rightStart = arg2.getStartOffset();

        byte s = getOrDeserializeTinyInt(leftBytes, leftStart, obj1);
        switch (typeTag2) {
            case TINYINT:
                return compareByte(s, getOrDeserializeTinyInt(rightBytes, rightStart, obj2));
            case SMALLINT:
                return compareShort(s, getOrDeserializeSmallInt(rightBytes, rightStart, obj2));
            case INTEGER:
                return compareInt(s, getOrDeserializeInt(rightBytes, rightStart, obj2));
            case BIGINT:
                return compareLong(s, getOrDeserializeBigInt(rightBytes, rightStart, obj2));
            case FLOAT:
                return compareFloat(s, getOrDeserializeFloat(rightBytes, rightStart, obj2));
            case DOUBLE:
                return compareDouble(s, getOrDeserializeDouble(rightBytes, rightStart, obj2));
            default:
                throw new IncompatibleTypeException(sourceLoc, COMPARISON, ATypeTag.SERIALIZED_INT8_TYPE_TAG,
                        typeTag2.serialize());
        }
    }

    private final byte getOrDeserializeTinyInt(byte[] bytes, int offset, Number obj) {
        return obj == null ? AInt8SerializerDeserializer.getByte(bytes, offset) : obj.byteValue();
    }

    private final short getOrDeserializeSmallInt(byte[] bytes, int offset, Number obj) {
        return obj == null ? AInt16SerializerDeserializer.getShort(bytes, offset) : obj.shortValue();
    }

    private final int getOrDeserializeInt(byte[] bytes, int offset, Number obj) {
        return obj == null ? AInt32SerializerDeserializer.getInt(bytes, offset) : obj.intValue();
    }

    private final long getOrDeserializeBigInt(byte[] bytes, int offset, Number obj) {
        return obj == null ? AInt64SerializerDeserializer.getLong(bytes, offset) : obj.longValue();
    }

    private final float getOrDeserializeFloat(byte[] bytes, int offset, Number obj) {
        return obj == null ? AFloatSerializerDeserializer.getFloat(bytes, offset) : obj.floatValue();
    }

    private final double getOrDeserializeDouble(byte[] bytes, int offset, Number obj) {
        return obj == null ? ADoubleSerializerDeserializer.getDouble(bytes, offset) : obj.doubleValue();
    }

    private final int compareByte(int v1, int v2) {
        if (v1 == v2) {
            return 0;
        } else if (v1 < v2) {
            return -1;
        } else {
            return 1;
        }
    }

    private final int compareShort(int v1, int v2) {
        if (v1 == v2) {
            return 0;
        } else if (v1 < v2) {
            return -1;
        } else {
            return 1;
        }
    }

    private final int compareInt(int v1, int v2) {
        if (v1 == v2) {
            return 0;
        } else if (v1 < v2) {
            return -1;
        } else {
            return 1;
        }
    }

    private final int compareLong(long v1, long v2) {
        if (v1 == v2) {
            return 0;
        } else if (v1 < v2) {
            return -1;
        } else {
            return 1;
        }
    }

    private final int compareFloat(float v1, float v2) {
        return Float.compare(v1, v2);
    }

    private final int compareDouble(double v1, double v2) {
        return Double.compare(v1, v2);
    }

    /**
     * When field value falls into the primitive type groups, we consider to cache its value instead of deserialize it
     * every time.
     *
     * @param bytes
     * @return primitive value as Number
     */
    public Number getNumberValue(byte[] bytes) {
        ATypeTag aTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[0]);
        int offset = 1;
        if (aTypeTag == null) {
            return null;
        }
        switch (aTypeTag) {
            case TINYINT:
                return AInt8SerializerDeserializer.getByte(bytes, offset);
            case SMALLINT:
                return AInt16SerializerDeserializer.getShort(bytes, offset);
            case INTEGER:
                return AInt32SerializerDeserializer.getInt(bytes, offset);
            case BIGINT:
                return AInt64SerializerDeserializer.getLong(bytes, offset);
            case FLOAT:
                return AFloatSerializerDeserializer.getFloat(bytes, offset);
            case DOUBLE:
                return ADoubleSerializerDeserializer.getDouble(bytes, offset);
            default:
                return null;
        }
    }
}
