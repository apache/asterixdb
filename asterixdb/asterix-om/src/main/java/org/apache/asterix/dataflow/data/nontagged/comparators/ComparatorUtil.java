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

import static org.apache.asterix.dataflow.data.common.ILogicalBinaryComparator.asResult;
import static org.apache.asterix.om.types.ATypeTag.BIGINT;
import static org.apache.asterix.om.types.ATypeTag.DOUBLE;
import static org.apache.asterix.om.types.ATypeTag.FLOAT;
import static org.apache.asterix.om.types.ATypeTag.INTEGER;
import static org.apache.asterix.om.types.ATypeTag.MISSING;
import static org.apache.asterix.om.types.ATypeTag.NULL;
import static org.apache.asterix.om.types.ATypeTag.SMALLINT;
import static org.apache.asterix.om.types.ATypeTag.TINYINT;

import org.apache.asterix.dataflow.data.common.ILogicalBinaryComparator;
import org.apache.asterix.dataflow.data.common.ILogicalBinaryComparator.Result;
import org.apache.asterix.dataflow.data.common.TaggedValueReference;
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

// TODO(ali): refactor some functionality with ATypeHierarchy and others
public class ComparatorUtil {

    private ComparatorUtil() {
    }

    public static ILogicalBinaryComparator createLogicalComparator(IAType left, IAType right, boolean isEquality) {
        IAType leftType = TypeComputeUtils.getActualType(left);
        IAType rightType = TypeComputeUtils.getActualType(right);

        if (leftType.getTypeTag().isDerivedType() && rightType.getTypeTag().isDerivedType()) {
            return new LogicalComplexBinaryComparator(leftType, rightType, isEquality);
        } else if (leftType.getTypeTag() == ATypeTag.ANY || rightType.getTypeTag() == ATypeTag.ANY) {
            return new LogicalGenericBinaryComparator(leftType, rightType, isEquality);
        } else {
            return LogicalScalarBinaryComparator.of(isEquality);
        }
    }

    static Result returnMissingOrNullOrMismatch(ATypeTag leftTag, ATypeTag rightTag) {
        if (leftTag == MISSING || rightTag == MISSING) {
            return Result.MISSING;
        }
        if (leftTag == NULL || rightTag == NULL) {
            return Result.NULL;
        }
        if (!ATypeHierarchy.isCompatible(leftTag, rightTag)) {
            return Result.INCOMPARABLE;
        }
        return null;
    }

    // start points to the value; checking left and right are compatible and numbers has to be done before calling this
    static int compareNumbers(ATypeTag lTag, byte[] l, int lStart, ATypeTag rTag, byte[] r, int rStart) {
        if (lTag == DOUBLE || rTag == DOUBLE) {
            return Double.compare(getDoubleValue(lTag, l, lStart), getDoubleValue(rTag, r, rStart));
        } else if (lTag == FLOAT || rTag == FLOAT) {
            return Float.compare((float) getDoubleValue(lTag, l, lStart), (float) getDoubleValue(rTag, r, rStart));
        } else if (lTag == BIGINT || rTag == BIGINT) {
            return Long.compare(getLongValue(lTag, l, lStart), getLongValue(rTag, r, rStart));
        } else if (lTag == INTEGER || lTag == SMALLINT || lTag == TINYINT) {
            return Integer.compare((int) getLongValue(lTag, l, lStart), (int) getLongValue(rTag, r, rStart));
        }
        // TODO(ali): use unsupported type
        throw new UnsupportedOperationException();
    }

    // checking that left and right are compatible has to be done before calling this
    static Result compareNumWithConstant(TaggedValueReference left, IAObject right) {
        ATypeTag leftTag = left.getTag();
        ATypeTag rightTag = right.getType().getTypeTag();
        byte[] leftBytes = left.getByteArray();
        int start = left.getStartOffset();
        if (leftTag == DOUBLE || rightTag == DOUBLE) {
            return asResult(Double.compare(getDoubleValue(leftTag, leftBytes, start), getConstantDouble(right)));
        } else if (leftTag == FLOAT || rightTag == FLOAT) {
            return asResult(
                    Float.compare((float) getDoubleValue(leftTag, leftBytes, start), (float) getConstantDouble(right)));
        } else if (leftTag == BIGINT || rightTag == BIGINT) {
            return asResult(Long.compare(getLongValue(leftTag, leftBytes, start), getConstantLong(right)));
        } else if (leftTag == INTEGER || leftTag == SMALLINT || leftTag == TINYINT) {
            return asResult(
                    Integer.compare((int) getLongValue(leftTag, leftBytes, start), (int) getConstantLong(right)));
        }
        // TODO(ali): use unsupported type
        throw new UnsupportedOperationException();
    }

    // checking that left and right are compatible has to be done before calling this
    static Result compareConstants(IAObject leftConstant, IAObject rightConstant) {
        ATypeTag leftTag = leftConstant.getType().getTypeTag();
        ATypeTag rightTag = rightConstant.getType().getTypeTag();
        if (leftTag == DOUBLE || rightTag == DOUBLE) {
            return asResult(Double.compare(getConstantDouble(leftConstant), getConstantDouble(rightConstant)));
        } else if (leftTag == FLOAT || rightTag == FLOAT) {
            return asResult(
                    Float.compare((float) getConstantDouble(leftConstant), (float) getConstantDouble(rightConstant)));
        } else if (leftTag == BIGINT || rightTag == BIGINT) {
            return asResult(Long.compare(getConstantLong(leftConstant), getConstantLong(rightConstant)));
        } else if (leftTag == INTEGER || leftTag == SMALLINT || leftTag == TINYINT) {
            return asResult(Integer.compare((int) getConstantLong(leftConstant), (int) getConstantLong(rightConstant)));
        }
        // TODO(ali): use unsupported type
        throw new UnsupportedOperationException();
    }

    private static double getDoubleValue(ATypeTag numericTag, byte[] bytes, int start) {
        switch (numericTag) {
            case TINYINT:
                return AInt8SerializerDeserializer.getByte(bytes, start);
            case SMALLINT:
                return AInt16SerializerDeserializer.getShort(bytes, start);
            case INTEGER:
                return AInt32SerializerDeserializer.getInt(bytes, start);
            case BIGINT:
                return AInt64SerializerDeserializer.getLong(bytes, start);
            case FLOAT:
                return AFloatSerializerDeserializer.getFloat(bytes, start);
            case DOUBLE:
                return ADoubleSerializerDeserializer.getDouble(bytes, start);
            default:
                // TODO(ali): use unsupported type
                throw new UnsupportedOperationException();
        }
    }

    private static long getLongValue(ATypeTag numericTag, byte[] bytes, int start) {
        switch (numericTag) {
            case TINYINT:
                return AInt8SerializerDeserializer.getByte(bytes, start);
            case SMALLINT:
                return AInt16SerializerDeserializer.getShort(bytes, start);
            case INTEGER:
                return AInt32SerializerDeserializer.getInt(bytes, start);
            case BIGINT:
                return AInt64SerializerDeserializer.getLong(bytes, start);
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
