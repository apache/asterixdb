/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.dataflow.data.nontagged.comparators;

import edu.uci.ics.asterix.formats.nontagged.UTF8StringLowercasePointable;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

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
            final IBinaryComparator rawComp = RawBinaryComparatorFactory.INSTANCE.createBinaryComparator();

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

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
                    default: {
                        return rawComp.compare(b1, s1 + skip1, l1 - skip1, b2, s2 + skip2, l2 - skip2);
                    }
                }
            }
        };
    }
}
