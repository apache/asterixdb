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
package org.apache.asterix.om.types.hierachy;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;

public abstract class AbstractIntegerTypeConvertComputer implements ITypeConvertComputer {

    private final boolean strict;

    protected AbstractIntegerTypeConvertComputer(boolean strict) {
        this.strict = strict;
    }

    // Refer to the following to convert byte array to integer types, and vice versa.
    // https://docs.oracle.com/javase/7/docs/api/java/io/DataOutput.html
    // https://docs.oracle.com/javase/7/docs/api/java/io/DataInput.html
    protected void convertIntegerType(byte[] data, int start, int length, DataOutput out, ATypeTag targetType,
            int targetTypeLength) throws IOException {
        long num = asLong(data, start, length);
        num = validate(num, targetType);
        out.writeByte(targetType.serialize());
        writeTargetValue(num, out, targetTypeLength);
    }

    static long asLong(byte[] data, int start, int length) throws RuntimeDataException {
        switch (length) {
            case 1: // TINYINT
                return BytePointable.getByte(data, start);
            case 2: // SMALLINT
                return ShortPointable.getShort(data, start);
            case 4: // INTEGER
                return IntegerPointable.getInteger(data, start);
            case 8: // BIGINT
                return LongPointable.getLong(data, start);
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_CONVERT_INTEGER_SOURCE, Arrays.toString(
                        new ATypeTag[] { ATypeTag.TINYINT, ATypeTag.SMALLINT, ATypeTag.INTEGER, ATypeTag.BIGINT }));
        }
    }

    private void writeTargetValue(long num, DataOutput out, int targetTypeLength) throws IOException {
        // Write actual target value
        switch (targetTypeLength) {
            case 1: // TINYINT
                out.writeByte((byte) (num & 0xff));
                break;

            case 2: // SMALLINT
                out.writeByte((byte) ((num >> 8) & 0xff));
                out.writeByte((byte) (num & 0xff));
                break;

            case 4: // INTEGER
                out.writeByte((byte) ((num >> 24) & 0xff));
                out.writeByte((byte) ((num >> 16) & 0xff));
                out.writeByte((byte) ((num >> 8) & 0xff));
                out.writeByte((byte) (num & 0xff));
                break;

            case 8: // BIGINT
                out.writeByte((byte) ((num >> 56) & 0xff));
                out.writeByte((byte) ((num >> 48) & 0xff));
                out.writeByte((byte) ((num >> 40) & 0xff));
                out.writeByte((byte) ((num >> 32) & 0xff));
                out.writeByte((byte) ((num >> 24) & 0xff));
                out.writeByte((byte) ((num >> 16) & 0xff));
                out.writeByte((byte) ((num >> 8) & 0xff));
                out.writeByte((byte) (num & 0xff));
                break;

            default:
                throw new RuntimeDataException(ErrorCode.TYPE_CONVERT_INTEGER_TARGET, Arrays.toString(
                        new ATypeTag[] { ATypeTag.TINYINT, ATypeTag.SMALLINT, ATypeTag.INTEGER, ATypeTag.BIGINT }));
        }
    }

    protected IAObject convertIntegerType(IAObject sourceObject, ATypeTag targetType) throws HyracksDataException {
        long num;

        switch (sourceObject.getType().getTypeTag()) {
            case TINYINT:
                num = ((AInt8) sourceObject).getByteValue();
                break;
            case SMALLINT:
                num = ((AInt16) sourceObject).getShortValue();
                break;
            case INTEGER:
                num = ((AInt32) sourceObject).getIntegerValue();
                break;
            case BIGINT:
                num = ((AInt64) sourceObject).getLongValue();
                break;
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_CONVERT_INTEGER_SOURCE, Arrays.toString(
                        new ATypeTag[] { ATypeTag.TINYINT, ATypeTag.SMALLINT, ATypeTag.INTEGER, ATypeTag.BIGINT }));
        }

        num = validate(num, targetType);

        switch (targetType) {
            case TINYINT:
                return new AInt8((byte) num);
            case SMALLINT:
                return new AInt16((short) num);
            case INTEGER:
                return new AInt32((int) num);
            case BIGINT:
                return new AInt64(num);
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_CONVERT_INTEGER_TARGET, Arrays.toString(
                        new ATypeTag[] { ATypeTag.TINYINT, ATypeTag.SMALLINT, ATypeTag.INTEGER, ATypeTag.BIGINT }));
        }
    }

    protected long convertIntegerType(byte[] data, int start, ATypeTag sourceTypeTag, ATypeTag targetType)
            throws HyracksDataException {
        long num;
        switch (sourceTypeTag) {
            case TINYINT:
                num = BytePointable.getByte(data, start);
                break;
            case SMALLINT:
                num = ShortPointable.getShort(data, start);
                break;
            case INTEGER:
                num = IntegerPointable.getInteger(data, start);
                break;
            case BIGINT:
                num = LongPointable.getLong(data, start);
                break;
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_CONVERT_INTEGER_SOURCE, Arrays.toString(
                        new ATypeTag[] { ATypeTag.TINYINT, ATypeTag.SMALLINT, ATypeTag.INTEGER, ATypeTag.BIGINT }));
        }

        return validate(num, targetType);
    }

    private long validate(long num, ATypeTag targetType) throws HyracksDataException {
        // Boundary check
        switch (targetType) {
            case TINYINT:
                if (num > Byte.MAX_VALUE) {
                    if (strict) {
                        raiseBoundaryException(num, targetType, Byte.MAX_VALUE, Byte.MIN_VALUE);
                    } else {
                        return Byte.MAX_VALUE;
                    }
                } else if (num < Byte.MIN_VALUE) {
                    if (strict) {
                        raiseBoundaryException(num, targetType, Byte.MAX_VALUE, Byte.MIN_VALUE);
                    } else {
                        return Byte.MIN_VALUE;
                    }
                }
                break;

            case SMALLINT:
                if (num > Short.MAX_VALUE) {
                    if (strict) {
                        raiseBoundaryException(num, targetType, Short.MAX_VALUE, Short.MIN_VALUE);
                    } else {
                        return Short.MAX_VALUE;
                    }
                } else if (num < Short.MIN_VALUE) {
                    if (strict) {
                        raiseBoundaryException(num, targetType, Short.MAX_VALUE, Short.MIN_VALUE);
                    } else {
                        return Short.MIN_VALUE;
                    }
                }
                break;

            case INTEGER:
                if (num > Integer.MAX_VALUE) {
                    if (strict) {
                        raiseBoundaryException(num, targetType, Integer.MAX_VALUE, Integer.MIN_VALUE);
                    } else {
                        return Integer.MAX_VALUE;
                    }
                } else if (num < Integer.MIN_VALUE) {
                    if (strict) {
                        raiseBoundaryException(num, targetType, Integer.MAX_VALUE, Integer.MIN_VALUE);
                    } else {
                        return Integer.MIN_VALUE;
                    }
                }
                break;

            case BIGINT:
            default:
                break;
        }

        return num;
    }

    private void raiseBoundaryException(long num, ATypeTag targetType, long maxValue, long minValue)
            throws HyracksDataException {
        throw new RuntimeDataException(ErrorCode.TYPE_CONVERT_OUT_OF_BOUND, num, targetType, maxValue, minValue);
    }
}
