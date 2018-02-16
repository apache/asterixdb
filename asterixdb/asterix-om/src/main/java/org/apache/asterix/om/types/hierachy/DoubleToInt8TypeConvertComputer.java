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
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy.TypeCastingMathFunctionType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.DoublePointable;

public class DoubleToInt8TypeConvertComputer implements ITypeConvertComputer {

    private static final DoubleToInt8TypeConvertComputer INSTANCE_STRICT = new DoubleToInt8TypeConvertComputer(true);

    private static final DoubleToInt8TypeConvertComputer INSTANCE_LAX = new DoubleToInt8TypeConvertComputer(false);

    private final boolean strict;

    private DoubleToInt8TypeConvertComputer(boolean strict) {
        this.strict = strict;
    }

    public static DoubleToInt8TypeConvertComputer getInstance(boolean strict) {
        return strict ? INSTANCE_STRICT : INSTANCE_LAX;
    }

    @Override
    public void convertType(byte[] data, int start, int length, DataOutput out) throws IOException {
        double sourceValue = DoublePointable.getDouble(data, start);
        byte targetValue = convert(sourceValue);
        out.writeByte(ATypeTag.TINYINT.serialize());
        out.writeByte(targetValue);
    }

    @Override
    public IAObject convertType(IAObject sourceObject, TypeCastingMathFunctionType mathFunction)
            throws HyracksDataException {
        double sourceValue = ATypeHierarchy.applyMathFunctionToDoubleValue(sourceObject, mathFunction);
        byte targetValue = convert(sourceValue);
        return new AInt8(targetValue);
    }

    private byte convert(double sourceValue) throws HyracksDataException {
        // Boundary check
        if (Double.isNaN(sourceValue)) {
            if (strict) {
                raiseBoundaryCheckException(sourceValue);
            } else {
                return 0;
            }
        }
        if (sourceValue > Byte.MAX_VALUE) {
            if (strict) {
                raiseBoundaryCheckException(sourceValue);
            } else {
                return Byte.MAX_VALUE;
            }
        } else if (sourceValue < Byte.MIN_VALUE) {
            if (strict) {
                raiseBoundaryCheckException(sourceValue);
            } else {
                return Byte.MIN_VALUE;
            }
        }

        // Math.floor to truncate decimal portion
        return (byte) Math.floor(sourceValue);
    }

    private void raiseBoundaryCheckException(double sourceValue) throws HyracksDataException {
        throw new RuntimeDataException(ErrorCode.TYPE_CONVERT_OUT_OF_BOUND, sourceValue, ATypeTag.TINYINT,
                Byte.MAX_VALUE, Byte.MIN_VALUE);
    }
}
