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

import java.io.IOException;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.types.hierachy.ITypeConvertComputer;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class AObjectAscBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;

    public static final AObjectAscBinaryComparatorFactory INSTANCE = new AObjectAscBinaryComparatorFactory();

    private AObjectAscBinaryComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return new ABinaryComparator() {

            // a storage to promote a value
            private ArrayBackedValueStorage castBuffer = new ArrayBackedValueStorage();
            private ITypeConvertComputer promoteComputer;

            // BOOLEAN
            final IBinaryComparator ascBoolComp = BooleanBinaryComparatorFactory.INSTANCE.createBinaryComparator();
            // INT8
            final IBinaryComparator ascByteComp = new PointableBinaryComparatorFactory(BytePointable.FACTORY)
                    .createBinaryComparator();
            // INT16
            final IBinaryComparator ascShortComp = new PointableBinaryComparatorFactory(ShortPointable.FACTORY)
                    .createBinaryComparator();
            // INT32
            final IBinaryComparator ascIntComp = new PointableBinaryComparatorFactory(IntegerPointable.FACTORY)
                    .createBinaryComparator();
            // INT64
            final IBinaryComparator ascLongComp = LongBinaryComparatorFactory.INSTANCE.createBinaryComparator();
            // STRING
            final IBinaryComparator ascStrComp = new PointableBinaryComparatorFactory(UTF8StringPointable.FACTORY)
                    .createBinaryComparator();
            // BINARY
            final IBinaryComparator ascByteArrayComp = new PointableBinaryComparatorFactory(ByteArrayPointable.FACTORY)
                    .createBinaryComparator();
            // FLOAT
            final IBinaryComparator ascFloatComp = new PointableBinaryComparatorFactory(FloatPointable.FACTORY)
                    .createBinaryComparator();
            // DOUBLE
            final IBinaryComparator ascDoubleComp = new PointableBinaryComparatorFactory(DoublePointable.FACTORY)
                    .createBinaryComparator();
            // RECTANGLE
            final IBinaryComparator ascRectangleComp = ARectanglePartialBinaryComparatorFactory.INSTANCE
                    .createBinaryComparator();
            // CIRCLE
            final IBinaryComparator ascCircleComp = ACirclePartialBinaryComparatorFactory.INSTANCE
                    .createBinaryComparator();
            // DURATION
            final IBinaryComparator ascDurationComp = ADurationPartialBinaryComparatorFactory.INSTANCE
                    .createBinaryComparator();
            // INTERVAL
            final IBinaryComparator ascIntervalComp = AIntervalPartialBinaryComparatorFactory.INSTANCE
                    .createBinaryComparator();
            // LINE
            final IBinaryComparator ascLineComp = ALinePartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
            // POINT
            final IBinaryComparator ascPointComp = APointPartialBinaryComparatorFactory.INSTANCE
                    .createBinaryComparator();
            // POINT3D
            final IBinaryComparator ascPoint3DComp = APoint3DPartialBinaryComparatorFactory.INSTANCE
                    .createBinaryComparator();
            // POLYGON
            final IBinaryComparator ascPolygonComp = APolygonPartialBinaryComparatorFactory.INSTANCE
                    .createBinaryComparator();
            // UUID
            final IBinaryComparator ascUUIDComp = AUUIDPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
            // RAW
            final IBinaryComparator rawComp = RawBinaryComparatorFactory.INSTANCE.createBinaryComparator();

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) throws HyracksDataException {

                // Normally, comparing between NULL and non-NULL values should return UNKNOWN as the result.
                // However, at this point, we assume that NULL check between two types is already done.
                // Therefore, inside this method, we return an order between two values even if one value is NULL.
                if (b1[s1] == ATypeTag.NULL.serialize()) {
                    if (b2[s2] == ATypeTag.NULL.serialize())
                        return 0;
                    else
                        return -1;
                } else {
                    if (b2[s2] == ATypeTag.NULL.serialize())
                        return 1;
                }

                ATypeTag tag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b1[s1]);
                ATypeTag tag2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b2[s2]);

                // If one of tag is null, that means we are dealing with an empty byte array in one side.
                // And, we don't need to continue. We just compare raw byte by byte.
                if (tag1 == null || tag2 == null) {
                    return rawComp.compare(b1, s1, l1, b2, s2, l2);
                }

                // If two type does not match, we identify the source and the target and
                // promote the source to the target type if they are compatible.
                ATypeTag sourceTypeTag = null;
                ATypeTag targetTypeTag = null;
                boolean areTwoTagsEqual = false;
                boolean typePromotionApplied = false;
                boolean leftValueChanged = false;

                if (tag1 != tag2) {
                    // tag1 can be promoted to tag2 (e.g. tag1: INT16, tag2: INT32)
                    if (ATypeHierarchy.canPromote(tag1, tag2)) {
                        sourceTypeTag = tag1;
                        targetTypeTag = tag2;
                        typePromotionApplied = true;
                        leftValueChanged = true;
                        // or tag2 can be promoted to tag1 (e.g. tag2: INT32, tag1: DOUBLE)
                    } else if (ATypeHierarchy.canPromote(tag2, tag1)) {
                        sourceTypeTag = tag2;
                        targetTypeTag = tag1;
                        typePromotionApplied = true;
                    }

                    // we promote the source to the target by using a promoteComputer
                    if (typePromotionApplied) {
                        castBuffer.reset();
                        promoteComputer = ATypeHierarchy.getTypePromoteComputer(sourceTypeTag, targetTypeTag);
                        if (promoteComputer != null) {
                            try {
                                if (leftValueChanged) {
                                    // left side is the source
                                    promoteComputer.convertType(b1, s1 + 1, l1 - 1, castBuffer.getDataOutput());
                                } else {
                                    // right side is the source
                                    promoteComputer.convertType(b2, s2 + 1, l2 - 1, castBuffer.getDataOutput());
                                }
                            } catch (IOException e) {
                                throw new HyracksDataException("ComparatorFactory - failed to promote the type:"
                                        + sourceTypeTag + " to the type:" + targetTypeTag);
                            }
                        } else {
                            // No appropriate typePromoteComputer.
                            throw new HyracksDataException("No appropriate typePromoteComputer exists for "
                                    + sourceTypeTag + " to the " + targetTypeTag + " type. Please check the code.");
                        }
                    }
                } else {
                    // tag1 == tag2.
                    sourceTypeTag = tag1;
                    targetTypeTag = tag1;
                    areTwoTagsEqual = true;
                }

                // If two tags are not compatible, then we compare raw byte by byte, including the type tag.
                // This is especially useful when we need to generate some order between any two types.
                if ((!areTwoTagsEqual && !typePromotionApplied)) {
                    return rawComp.compare(b1, s1, l1, b2, s2, l2);
                }

                // Conduct actual compare()
                switch (targetTypeTag) {
                    case UUID:
                        return ascUUIDComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                    case BOOLEAN: {
                        return ascBoolComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                    }
                    case INT8: {
                        // No type promotion from another type to the INT8 can happen
                        return ascByteComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                    }
                    case INT16: {
                        if (!typePromotionApplied) {
                            // No type promotion case
                            return ascShortComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                        } else if (leftValueChanged) {
                            // Type promotion happened. Left side was the source
                            return ascShortComp.compare(castBuffer.getByteArray(), castBuffer.getStartOffset() + 1,
                                    castBuffer.getLength() - 1, b2, s2 + 1, l2 - 1);
                        } else {
                            // Type promotion happened. Right side was the source
                            return ascShortComp.compare(b1, s1 + 1, l1 - 1, castBuffer.getByteArray(),
                                    castBuffer.getStartOffset() + 1, castBuffer.getLength() - 1);
                        }
                    }
                    case TIME:
                    case DATE:
                    case YEARMONTHDURATION:
                    case INT32: {
                        if (!typePromotionApplied) {
                            // No type promotion case
                            return ascIntComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                        } else if (leftValueChanged) {
                            // Type promotion happened. Left side was the source
                            return ascIntComp.compare(castBuffer.getByteArray(), castBuffer.getStartOffset() + 1,
                                    castBuffer.getLength() - 1, b2, s2 + 1, l2 - 1);
                        } else {
                            // Type promotion happened. Right side was the source
                            return ascIntComp.compare(b1, s1 + 1, l1 - 1, castBuffer.getByteArray(),
                                    castBuffer.getStartOffset() + 1, castBuffer.getLength() - 1);
                        }
                    }
                    case DATETIME:
                    case DAYTIMEDURATION:
                    case INT64: {
                        if (!typePromotionApplied) {
                            // No type promotion case
                            return ascLongComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                        } else if (leftValueChanged) {
                            // Type promotion happened. Left side was the source
                            return ascLongComp.compare(castBuffer.getByteArray(), castBuffer.getStartOffset() + 1,
                                    castBuffer.getLength() - 1, b2, s2 + 1, l2 - 1);
                        } else {
                            // Type promotion happened. Right side was the source
                            return ascLongComp.compare(b1, s1 + 1, l1 - 1, castBuffer.getByteArray(),
                                    castBuffer.getStartOffset() + 1, castBuffer.getLength() - 1);
                        }
                    }
                    case FLOAT: {
                        if (!typePromotionApplied) {
                            // No type promotion case
                            return ascFloatComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                        } else if (leftValueChanged) {
                            // Type promotion happened. Left side was the source
                            return ascFloatComp.compare(castBuffer.getByteArray(), castBuffer.getStartOffset() + 1,
                                    castBuffer.getLength() - 1, b2, s2 + 1, l2 - 1);
                        } else {
                            // Type promotion happened. Right side was the source
                            return ascFloatComp.compare(b1, s1 + 1, l1 - 1, castBuffer.getByteArray(),
                                    castBuffer.getStartOffset() + 1, castBuffer.getLength() - 1);
                        }
                    }
                    case DOUBLE: {
                        if (!typePromotionApplied) {
                            // No type promotion case
                            return ascDoubleComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                        } else if (leftValueChanged) {
                            // Type promotion happened. Left side was the source
                            return ascDoubleComp.compare(castBuffer.getByteArray(), castBuffer.getStartOffset() + 1,
                                    castBuffer.getLength() - 1, b2, s2 + 1, l2 - 1);
                        } else {
                            // Type promotion happened. Right side was the source
                            return ascDoubleComp.compare(b1, s1 + 1, l1 - 1, castBuffer.getByteArray(),
                                    castBuffer.getStartOffset() + 1, castBuffer.getLength() - 1);
                        }
                    }
                    case STRING: {
                        return ascStrComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                    }
                    case RECTANGLE: {
                        return ascRectangleComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                    }
                    case CIRCLE: {
                        return ascCircleComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                    }
                    case POINT: {
                        return ascPointComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                    }
                    case POINT3D: {
                        return ascPoint3DComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                    }
                    case LINE: {
                        return ascLineComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                    }
                    case POLYGON: {
                        return ascPolygonComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                    }
                    case DURATION: {
                        return ascDurationComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                    }
                    case INTERVAL: {
                        return ascIntervalComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                    }
                    case BINARY: {
                        return ascByteArrayComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                    }
                    default: {
                        // We include typeTag in comparison to compare between two type to enforce some ordering
                        return rawComp.compare(b1, s1, l1, b2, s2, l2);
                    }
                }
            }
        };
    }
}
