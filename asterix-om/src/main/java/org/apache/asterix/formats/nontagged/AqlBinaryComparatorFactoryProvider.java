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
package org.apache.asterix.formats.nontagged;

import java.io.Serializable;

import org.apache.asterix.dataflow.data.nontagged.comparators.ABinaryComparator;
import org.apache.asterix.dataflow.data.nontagged.comparators.ACirclePartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ADurationPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AIntervalPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ALinePartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AObjectAscBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AObjectDescBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.APoint3DPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.APointPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.APolygonPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.ARectanglePartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AUUIDPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.BooleanBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.RawBinaryComparatorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.RawUTF8StringPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;

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
    public static final PointableBinaryComparatorFactory BINARY_POINTABLE_INSTANCE = new PointableBinaryComparatorFactory(
            ByteArrayPointable.FACTORY);

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
        // During a comparison, since proper type promotion among several numeric types are required,
        // we will use AObjectAscBinaryComparatorFactory, instead of using a specific comparator
        return anyBinaryComparatorFactory(ascending);
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
            case UUID: {
                return addOffset(AUUIDPartialBinaryComparatorFactory.INSTANCE, ascending);
            }
            case BINARY: {
                return addOffset(BINARY_POINTABLE_INSTANCE, ascending);
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
                    return new ABinaryComparator() {

                        @Override
                        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
                                throws HyracksDataException {
                            return bc.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                        }
                    };
                } else {
                    return new ABinaryComparator() {
                        @Override
                        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
                                throws HyracksDataException {
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
