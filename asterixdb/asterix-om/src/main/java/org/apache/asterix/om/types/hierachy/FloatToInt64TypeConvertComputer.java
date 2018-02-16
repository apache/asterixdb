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

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy.TypeCastingMathFunctionType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.FloatPointable;

public class FloatToInt64TypeConvertComputer implements ITypeConvertComputer {

    private static final FloatToInt64TypeConvertComputer INSTANCE_STRICT = new FloatToInt64TypeConvertComputer(true);

    private static final FloatToInt64TypeConvertComputer INSTANCE_LAX = new FloatToInt64TypeConvertComputer(false);

    private final boolean strict;

    private FloatToInt64TypeConvertComputer(boolean strict) {
        this.strict = strict;
    }

    public static FloatToInt64TypeConvertComputer getInstance(boolean strict) {
        return strict ? INSTANCE_STRICT : INSTANCE_LAX;
    }

    @Override
    public void convertType(byte[] data, int start, int length, DataOutput out) throws IOException {
        long targetValue = convertType(data, start);
        out.writeByte(ATypeTag.BIGINT.serialize());
        out.writeLong(targetValue);
    }

    @Override
    public IAObject convertType(IAObject sourceObject, TypeCastingMathFunctionType mathFunction)
            throws HyracksDataException {
        float sourceValue = ATypeHierarchy.applyMathFunctionToFloatValue(sourceObject, mathFunction);
        long targetValue = convert(sourceValue);
        return new AInt64(targetValue);
    }

    long convertType(byte[] data, int start) throws HyracksDataException {
        float sourceValue = FloatPointable.getFloat(data, start);
        return convert(sourceValue);
    }

    private long convert(float sourceValue) throws HyracksDataException {
        // Boundary check
        if (Float.isNaN(sourceValue)) {
            if (strict) {
                raiseBoundaryCheckException(sourceValue);
            } else {
                return 0;
            }
        } else if (sourceValue > Long.MAX_VALUE) {
            if (strict) {
                raiseBoundaryCheckException(sourceValue);
            } else {
                return Long.MAX_VALUE;
            }
        } else if (sourceValue < Long.MIN_VALUE) {
            if (strict) {
                raiseBoundaryCheckException(sourceValue);
            } else {
                return Long.MIN_VALUE;
            }
        }

        // Math.floor to truncate decimal portion
        return (long) Math.floor(sourceValue);
    }

    private void raiseBoundaryCheckException(float sourceValue) throws HyracksDataException {
        throw new RuntimeDataException(ErrorCode.TYPE_CONVERT_OUT_OF_BOUND, sourceValue, ATypeTag.BIGINT,
                Long.MAX_VALUE, Long.MIN_VALUE);
    }
}
