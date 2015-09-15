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

import org.apache.asterix.formats.nontagged.UTF8StringLowercasePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

public class ListItemBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;

    public static final ListItemBinaryComparatorFactory INSTANCE = new ListItemBinaryComparatorFactory();

    private ListItemBinaryComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return createBinaryComparator(ATypeTag.NULL, ATypeTag.NULL, false);
    }

    public IBinaryComparator createBinaryComparator(final ATypeTag firstItemTypeTag, final ATypeTag secondItemTypeTag,
            final boolean ignoreCase) {
        return new IBinaryComparator() {
            final IBinaryComparator ascBoolComp = BooleanBinaryComparatorFactory.INSTANCE.createBinaryComparator();
            final IBinaryComparator ascIntComp = new PointableBinaryComparatorFactory(IntegerPointable.FACTORY)
                    .createBinaryComparator();
            final IBinaryComparator ascLongComp = LongBinaryComparatorFactory.INSTANCE.createBinaryComparator();
            final IBinaryComparator ascStrComp = new PointableBinaryComparatorFactory(UTF8StringPointable.FACTORY)
                    .createBinaryComparator();
            final IBinaryComparator ascLowerCaseStrComp = new PointableBinaryComparatorFactory(
                    UTF8StringLowercasePointable.FACTORY).createBinaryComparator();
            final IBinaryComparator ascFloatComp = new PointableBinaryComparatorFactory(FloatPointable.FACTORY)
                    .createBinaryComparator();
            final IBinaryComparator ascDoubleComp = new PointableBinaryComparatorFactory(DoublePointable.FACTORY)
                    .createBinaryComparator();
            final IBinaryComparator ascRectangleComp = ARectanglePartialBinaryComparatorFactory.INSTANCE
                    .createBinaryComparator();
            final IBinaryComparator ascCircleComp = ACirclePartialBinaryComparatorFactory.INSTANCE
                    .createBinaryComparator();
            final IBinaryComparator ascDurationComp = ADurationPartialBinaryComparatorFactory.INSTANCE
                    .createBinaryComparator();
            final IBinaryComparator ascIntervalComp = AIntervalPartialBinaryComparatorFactory.INSTANCE
                    .createBinaryComparator();
            final IBinaryComparator ascLineComp = ALinePartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
            final IBinaryComparator ascPointComp = APointPartialBinaryComparatorFactory.INSTANCE
                    .createBinaryComparator();
            final IBinaryComparator ascPoint3DComp = APoint3DPartialBinaryComparatorFactory.INSTANCE
                    .createBinaryComparator();
            final IBinaryComparator ascPolygonComp = APolygonPartialBinaryComparatorFactory.INSTANCE
                    .createBinaryComparator();
            final IBinaryComparator ascUUIDComp = AUUIDPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
            final IBinaryComparator ascByteArrayComp = new PointableBinaryComparatorFactory(ByteArrayPointable.FACTORY)
                    .createBinaryComparator();
            final IBinaryComparator rawComp = RawBinaryComparatorFactory.INSTANCE.createBinaryComparator();

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) throws HyracksDataException {

                if (b1[s1] == ATypeTag.NULL.serialize()) {
                    if (b2[s2] == ATypeTag.NULL.serialize())
                        return 0;
                    else
                        return -1;
                } else {
                    if (b2[s2] == ATypeTag.NULL.serialize())
                        return 1;
                }

                ATypeTag tag1 = firstItemTypeTag;
                int skip1 = 0;
                if (firstItemTypeTag == ATypeTag.ANY) {
                    tag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b1[s1]);
                    skip1 = 1;
                }

                ATypeTag tag2 = secondItemTypeTag;
                int skip2 = 0;
                if (secondItemTypeTag == ATypeTag.ANY) {
                    tag2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b2[s2]);
                    skip2 = 1;
                }

                if (tag1 != tag2) {
                    return rawComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                }

                switch (tag1) {
                    case UUID: {
                        return ascUUIDComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                    }
                    case BOOLEAN: {
                        return ascBoolComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                    }
                    case TIME:
                    case DATE:
                    case YEARMONTHDURATION:
                    case INT32: {
                        return ascIntComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                    }
                    case DATETIME:
                    case DAYTIMEDURATION:
                    case INT64: {
                        return ascLongComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                    }
                    case FLOAT: {
                        return ascFloatComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                    }
                    case DOUBLE: {
                        return ascDoubleComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                    }
                    case STRING: {
                        if (ignoreCase) {
                            return ascLowerCaseStrComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                        } else {
                            return ascStrComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                        }
                    }
                    case RECTANGLE: {
                        return ascRectangleComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                    }
                    case CIRCLE: {
                        return ascCircleComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                    }
                    case POINT: {
                        return ascPointComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                    }
                    case POINT3D: {
                        return ascPoint3DComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                    }
                    case LINE: {
                        return ascLineComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                    }
                    case POLYGON: {
                        return ascPolygonComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                    }
                    case DURATION: {
                        return ascDurationComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                    }
                    case INTERVAL: {
                        return ascIntervalComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                    }
                    case BINARY: {
                        return ascByteArrayComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                    }
                    default: {
                        return rawComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                    }
                }
            }
        };
    }
}
