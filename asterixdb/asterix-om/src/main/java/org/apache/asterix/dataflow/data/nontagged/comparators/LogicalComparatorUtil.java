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

import static org.apache.asterix.om.types.ATypeTag.BIGINT;
import static org.apache.asterix.om.types.ATypeTag.DOUBLE;
import static org.apache.asterix.om.types.ATypeTag.FLOAT;
import static org.apache.asterix.om.types.ATypeTag.INTEGER;
import static org.apache.asterix.om.types.ATypeTag.MISSING;
import static org.apache.asterix.om.types.ATypeTag.NULL;
import static org.apache.asterix.om.types.ATypeTag.SMALLINT;
import static org.apache.asterix.om.types.ATypeTag.TINYINT;

import org.apache.asterix.dataflow.data.common.ILogicalBinaryComparator;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;

public class LogicalComparatorUtil {

    private LogicalComparatorUtil() {
    }

    public static ILogicalBinaryComparator createLogicalComparator(IAType left, IAType right, boolean isEquality) {
        IAType leftType = TypeComputeUtils.getActualType(left);
        IAType rightType = TypeComputeUtils.getActualType(right);

        // TODO(ali): after making comparators in scalar comparator stateless, create an INSTANCE only and use it here
        if (leftType.getTypeTag().isDerivedType() && rightType.getTypeTag().isDerivedType()) {
            return new LogicalComplexBinaryComparator(leftType, rightType, isEquality);
        } else if (leftType.getTypeTag() == ATypeTag.ANY || rightType.getTypeTag() == ATypeTag.ANY) {
            return new LogicalGenericBinaryComparator(leftType, rightType, isEquality);
        } else {
            return new LogicalScalarBinaryComparator(isEquality);
        }
    }

    static ILogicalBinaryComparator.Result returnMissingOrNullOrMismatch(ATypeTag leftTag, ATypeTag rightTag) {
        if (leftTag == MISSING || rightTag == MISSING) {
            return ILogicalBinaryComparator.Result.MISSING;
        }
        if (leftTag == NULL || rightTag == NULL) {
            return ILogicalBinaryComparator.Result.NULL;
        }
        if (!ATypeHierarchy.isCompatible(leftTag, rightTag)) {
            return ILogicalBinaryComparator.Result.INCOMPARABLE;
        }
        return null;
    }

    // checking that left and right are compatible has to be done before calling this
    static ILogicalBinaryComparator.Result compareNumbers(ATypeTag leftTag, byte[] b1, int s1, ATypeTag rightTag,
            byte[] b2, int s2) {
        int result;
        if (leftTag == DOUBLE || rightTag == DOUBLE) {
            result = Double.compare(getDoubleValue(leftTag, b1, s1), getDoubleValue(rightTag, b2, s2));
        } else if (leftTag == FLOAT || rightTag == FLOAT) {
            result = Float.compare((float) getDoubleValue(leftTag, b1, s1), (float) getDoubleValue(rightTag, b2, s2));
        } else if (leftTag == BIGINT || rightTag == BIGINT) {
            result = Long.compare(getLongValue(leftTag, b1, s1), getLongValue(rightTag, b2, s2));
        } else if (leftTag == INTEGER || leftTag == SMALLINT || leftTag == TINYINT) {
            result = Integer.compare((int) getLongValue(leftTag, b1, s1), (int) getLongValue(rightTag, b2, s2));
        } else {
            return null;
        }
        return ILogicalBinaryComparator.asResult(result);
    }

    // checking that left and right are compatible has to be done before calling this
    static ILogicalBinaryComparator.Result compareNumWithConstant(ATypeTag leftTag, byte[] b1, int s1,
            IAObject rightConstant) {
        int result;
        ATypeTag rightTag = rightConstant.getType().getTypeTag();
        if (leftTag == DOUBLE || rightTag == DOUBLE) {
            result = Double.compare(getDoubleValue(leftTag, b1, s1), getConstantDouble(rightConstant));
        } else if (leftTag == FLOAT || rightTag == FLOAT) {
            result = Float.compare((float) getDoubleValue(leftTag, b1, s1), (float) getConstantDouble(rightConstant));
        } else if (leftTag == BIGINT || rightTag == BIGINT) {
            result = Long.compare(getLongValue(leftTag, b1, s1), getConstantLong(rightConstant));
        } else if (leftTag == INTEGER || leftTag == SMALLINT || leftTag == TINYINT) {
            result = Integer.compare((int) getLongValue(leftTag, b1, s1), (int) getConstantLong(rightConstant));
        } else {
            return null;
        }
        return ILogicalBinaryComparator.asResult(result);
    }

    // checking that left and right are compatible has to be done before calling this
    static ILogicalBinaryComparator.Result compareConstants(IAObject leftConstant, IAObject rightConstant) {
        int result;
        ATypeTag leftTag = leftConstant.getType().getTypeTag();
        ATypeTag rightTag = rightConstant.getType().getTypeTag();
        if (leftTag == DOUBLE || rightTag == DOUBLE) {
            result = Double.compare(getConstantDouble(leftConstant), getConstantDouble(rightConstant));
        } else if (leftTag == FLOAT || rightTag == FLOAT) {
            result = Float.compare((float) getConstantDouble(leftConstant), (float) getConstantDouble(rightConstant));
        } else if (leftTag == BIGINT || rightTag == BIGINT) {
            result = Long.compare(getConstantLong(leftConstant), getConstantLong(rightConstant));
        } else if (leftTag == INTEGER || leftTag == SMALLINT || leftTag == TINYINT) {
            result = Integer.compare((int) getConstantLong(leftConstant), (int) getConstantLong(rightConstant));
        } else {
            return null;
        }
        return ILogicalBinaryComparator.asResult(result);
    }

    @SuppressWarnings("squid:S1226") // asking for introducing a new variable for s
    private static double getDoubleValue(ATypeTag numericTag, byte[] b, int s) {
        s++;
        switch (numericTag) {
            case TINYINT:
                return AInt8SerializerDeserializer.getByte(b, s);
            case SMALLINT:
                return AInt16SerializerDeserializer.getShort(b, s);
            case INTEGER:
                return AInt32SerializerDeserializer.getInt(b, s);
            case BIGINT:
                return AInt64SerializerDeserializer.getLong(b, s);
            case FLOAT:
                return AFloatSerializerDeserializer.getFloat(b, s);
            case DOUBLE:
                return ADoubleSerializerDeserializer.getDouble(b, s);
            default:
                // TODO(ali): use unsupported type
                throw new UnsupportedOperationException();
        }
    }

    @SuppressWarnings("squid:S1226") // asking for introducing a new variable for s
    private static long getLongValue(ATypeTag numericTag, byte[] b, int s) {
        s++;
        switch (numericTag) {
            case TINYINT:
                return AInt8SerializerDeserializer.getByte(b, s);
            case SMALLINT:
                return AInt16SerializerDeserializer.getShort(b, s);
            case INTEGER:
                return AInt32SerializerDeserializer.getInt(b, s);
            case BIGINT:
                return AInt64SerializerDeserializer.getLong(b, s);
            default:
                // TODO(ali): use unsupported type
                throw new UnsupportedOperationException();
        }
    }

    private static double getConstantDouble(IAObject numeric) {
        ATypeTag tag = numeric.getType().getTypeTag();
        switch (tag) {
            case DOUBLE:
                return ((ADouble) numeric).getDoubleValue();
            case FLOAT:
                return ((AFloat) numeric).getFloatValue();
            case BIGINT:
                return ((AInt64) numeric).getLongValue();
            case INTEGER:
                return ((AInt32) numeric).getIntegerValue();
            case SMALLINT:
                return ((AInt16) numeric).getShortValue();
            case TINYINT:
                return ((AInt8) numeric).getByteValue();
            default:
                // TODO(ali): use unsupported type
                throw new UnsupportedOperationException();
        }
    }

    private static long getConstantLong(IAObject numeric) {
        ATypeTag tag = numeric.getType().getTypeTag();
        switch (tag) {
            case BIGINT:
                return ((AInt64) numeric).getLongValue();
            case INTEGER:
                return ((AInt32) numeric).getIntegerValue();
            case SMALLINT:
                return ((AInt16) numeric).getShortValue();
            case TINYINT:
                return ((AInt8) numeric).getByteValue();
            default:
                // TODO(ali): use unsupported type
                throw new UnsupportedOperationException();
        }
    }
}
