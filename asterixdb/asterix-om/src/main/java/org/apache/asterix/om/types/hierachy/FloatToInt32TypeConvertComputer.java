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
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy.TypeCastingMathFunctionType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.FloatPointable;

public class FloatToInt32TypeConvertComputer implements ITypeConvertComputer {

    private static final FloatToInt32TypeConvertComputer INSTANCE_STRICT = new FloatToInt32TypeConvertComputer(true);

    private static final FloatToInt32TypeConvertComputer INSTANCE_LAX = new FloatToInt32TypeConvertComputer(false);

    private final boolean strict;

    private FloatToInt32TypeConvertComputer(boolean strict) {
        this.strict = strict;
    }

    public static FloatToInt32TypeConvertComputer getInstance(boolean strict) {
        return strict ? INSTANCE_STRICT : INSTANCE_LAX;
    }

    @Override
    public void convertType(byte[] data, int start, int length, DataOutput out) throws IOException {
        int targetValue = convertType(data, start);
        out.writeByte(ATypeTag.INTEGER.serialize());
        out.writeInt(targetValue);
    }

    int convertType(byte data[], int start) throws HyracksDataException {
        float sourceValue = FloatPointable.getFloat(data, start);
        return convert(sourceValue);
    }

    @Override
    public IAObject convertType(IAObject sourceObject, TypeCastingMathFunctionType mathFunction)
            throws HyracksDataException {
        float sourceValue = ATypeHierarchy.applyMathFunctionToFloatValue(sourceObject, mathFunction);
        int targetValue = convert(sourceValue);
        return new AInt32(targetValue);
    }

    private int convert(float sourceValue) throws HyracksDataException {
        // Boundary check
        if (Float.isNaN(sourceValue)) {
            if (strict) {
                raiseBoundaryCheckException(sourceValue);
            } else {
                return 0;
            }
        } else if (sourceValue > Integer.MAX_VALUE) {
            if (strict) {
                raiseBoundaryCheckException(sourceValue);
            } else {
                return Integer.MAX_VALUE;
            }
        } else if (sourceValue < Integer.MIN_VALUE) {
            if (strict) {
                raiseBoundaryCheckException(sourceValue);
            } else {
                return Integer.MIN_VALUE;
            }
        }

        // Math.floor to truncate decimal portion
        return (int) Math.floor(sourceValue);
    }

    private void raiseBoundaryCheckException(float sourceValue) throws HyracksDataException {
        throw new RuntimeDataException(ErrorCode.TYPE_CONVERT_OUT_OF_BOUND, sourceValue, ATypeTag.INTEGER,
                Integer.MAX_VALUE, Integer.MIN_VALUE);
    }
}
