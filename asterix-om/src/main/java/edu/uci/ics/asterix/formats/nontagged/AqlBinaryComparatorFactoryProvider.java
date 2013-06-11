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
package edu.uci.ics.asterix.formats.nontagged;

import java.io.Serializable;

import edu.uci.ics.asterix.dataflow.data.nontagged.comparators.ACirclePartialBinaryComparatorFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.comparators.ADurationPartialBinaryComparatorFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.comparators.AIntervalPartialBinaryComparatorFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.comparators.ALinePartialBinaryComparatorFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.comparators.AObjectAscBinaryComparatorFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.comparators.AObjectDescBinaryComparatorFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.comparators.APoint3DPartialBinaryComparatorFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.comparators.APointPartialBinaryComparatorFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.comparators.APolygonPartialBinaryComparatorFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.comparators.BooleanBinaryComparatorFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.comparators.ARectanglePartialBinaryComparatorFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.comparators.RawBinaryComparatorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.RawUTF8StringPointable;
import edu.uci.ics.hyracks.data.std.primitive.ShortPointable;

public class AqlBinaryComparatorFactoryProvider implements IBinaryComparatorFactoryProvider, Serializable {

    private static final long serialVersionUID = 1L;
    public static final AqlBinaryComparatorFactoryProvider INSTANCE = new AqlBinaryComparatorFactoryProvider();
    public static final PointableBinaryComparatorFactory BYTE_POINTABLE_INSTANCE = new PointableBinaryComparatorFactory(
            BytePointable.FACTORY);
    public static final PointableBinaryComparatorFactory SHORT_POINTABLE_INSTANCE = new PointableBinaryComparatorFactory(
            ShortPointable.FACTORY);
    public static final PointableBinaryComparatorFactory INTEGER_POINTABLE_INSTANCE = new PointableBinaryComparatorFactory(
            IntegerPointable.FACTORY);
    public static final PointableBinaryComparatorFactory LONG_POINTABLE_INSTANCE = new PointableBinaryComparatorFactory(
            LongPointable.FACTORY);
    public static final PointableBinaryComparatorFactory FLOAT_POINTABLE_INSTANCE = new PointableBinaryComparatorFactory(
            FloatPointable.FACTORY);
    public static final PointableBinaryComparatorFactory DOUBLE_POINTABLE_INSTANCE = new PointableBinaryComparatorFactory(
            DoublePointable.FACTORY);
    public static final PointableBinaryComparatorFactory UTF8STRING_POINTABLE_INSTANCE = new PointableBinaryComparatorFactory(
            RawUTF8StringPointable.FACTORY);
    // Equivalent to UTF8STRING_POINTABLE_INSTANCE but all characters are considered lower case to implement case-insensitive comparisons.    
    public static final PointableBinaryComparatorFactory UTF8STRING_LOWERCASE_POINTABLE_INSTANCE = new PointableBinaryComparatorFactory(
            UTF8StringLowercasePointable.FACTORY);

    private AqlBinaryComparatorFactoryProvider() {
    }

    // This method add the option of ignoring the case in string comparisons.
    // TODO: We should incorporate this option more nicely, but I'd have to change algebricks.
    public IBinaryComparatorFactory getBinaryComparatorFactory(Object type, boolean ascending, boolean ignoreCase) {
        if (type == null) {
            return anyBinaryComparatorFactory(ascending);
        }
        IAType aqlType = (IAType) type;
        if (aqlType.getTypeTag() == ATypeTag.STRING && ignoreCase) {
            return addOffset(UTF8STRING_LOWERCASE_POINTABLE_INSTANCE, ascending);
        }
        return getBinaryComparatorFactory(type, ascending);
    }

    @Override
    public IBinaryComparatorFactory getBinaryComparatorFactory(Object type, boolean ascending) {
        if (type == null) {
            return anyBinaryComparatorFactory(ascending);
        }
        IAType aqlType = (IAType) type;
        return getBinaryComparatorFactory(aqlType.getTypeTag(), ascending);
    }

    public IBinaryComparatorFactory getBinaryComparatorFactory(ATypeTag type, boolean ascending) {
        switch (type) {
            case ANY:
            case UNION: { // we could do smth better for nullable fields
                return anyBinaryComparatorFactory(ascending);
            }
            case NULL: {
                return new IBinaryComparatorFactory() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public IBinaryComparator createBinaryComparator() {
                        return new IBinaryComparator() {

                            @Override
                            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                                return 0;
                            }
                        };
                    }
                };
            }
            case BOOLEAN: {
                return addOffset(BooleanBinaryComparatorFactory.INSTANCE, ascending);
            }
            case INT8: {
                return addOffset(BYTE_POINTABLE_INSTANCE, ascending);
            }
            case INT16: {
                return addOffset(SHORT_POINTABLE_INSTANCE, ascending);
            }
            case DATE:
            case TIME:
            case YEARMONTHDURATION:
            case INT32: {
                return addOffset(INTEGER_POINTABLE_INSTANCE, ascending);
            }
            case DATETIME:
            case DAYTIMEDURATION:
            case INT64: {
                return addOffset(LONG_POINTABLE_INSTANCE, ascending);
            }
            case FLOAT: {
                return addOffset(FLOAT_POINTABLE_INSTANCE, ascending);
            }
            case DOUBLE: {
                return addOffset(DOUBLE_POINTABLE_INSTANCE, ascending);
            }
            case STRING: {
                return addOffset(UTF8STRING_POINTABLE_INSTANCE, ascending);
            }
            case RECTANGLE: {
                return addOffset(ARectanglePartialBinaryComparatorFactory.INSTANCE, ascending);
            }
            case CIRCLE: {
                return addOffset(ACirclePartialBinaryComparatorFactory.INSTANCE, ascending);
            }
            case POINT: {
                return addOffset(APointPartialBinaryComparatorFactory.INSTANCE, ascending);
            }
            case POINT3D: {
                return addOffset(APoint3DPartialBinaryComparatorFactory.INSTANCE, ascending);
            }
            case LINE: {
                return addOffset(ALinePartialBinaryComparatorFactory.INSTANCE, ascending);
            }
            case POLYGON: {
                return addOffset(APolygonPartialBinaryComparatorFactory.INSTANCE, ascending);
            }
            case DURATION: {
                return addOffset(ADurationPartialBinaryComparatorFactory.INSTANCE, ascending);
            }
            case INTERVAL: {
                return addOffset(AIntervalPartialBinaryComparatorFactory.INSTANCE, ascending);
            }
            default: {
                return addOffset(RawBinaryComparatorFactory.INSTANCE, ascending);
            }
        }
    }

    private IBinaryComparatorFactory addOffset(final IBinaryComparatorFactory inst, final boolean ascending) {
        return new IBinaryComparatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IBinaryComparator createBinaryComparator() {
                final IBinaryComparator bc = inst.createBinaryComparator();
                if (ascending) {
                    return new IBinaryComparator() {

                        @Override
                        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                            return bc.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                        }
                    };
                } else {
                    return new IBinaryComparator() {
                        @Override
                        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                            return -bc.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                        }
                    };
                }
            }
        };
    }

    private IBinaryComparatorFactory anyBinaryComparatorFactory(boolean ascending) {
        if (ascending) {
            return AObjectAscBinaryComparatorFactory.INSTANCE;
        } else {
            return AObjectDescBinaryComparatorFactory.INSTANCE;
        }
    }
}
