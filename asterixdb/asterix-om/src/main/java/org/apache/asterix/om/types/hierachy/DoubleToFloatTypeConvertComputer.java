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
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy.TypeCastingMathFunctionType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.DoublePointable;

public class DoubleToFloatTypeConvertComputer implements ITypeConvertComputer {

    private static final DoubleToFloatTypeConvertComputer INSTANCE_STRICT = new DoubleToFloatTypeConvertComputer(true);

    private static final DoubleToFloatTypeConvertComputer INSTANCE_LAX = new DoubleToFloatTypeConvertComputer(false);

    private final boolean strict;

    private DoubleToFloatTypeConvertComputer(boolean strict) {
        this.strict = strict;
    }

    public static DoubleToFloatTypeConvertComputer getInstance(boolean strict) {
        return strict ? INSTANCE_STRICT : INSTANCE_LAX;
    }

    @Override
    public void convertType(byte[] data, int start, int length, DataOutput out) throws IOException {
        double sourceValue = DoublePointable.getDouble(data, start);
        float targetValue = convert(sourceValue);
        out.writeByte(ATypeTag.FLOAT.serialize());
        out.writeFloat(targetValue);
    }

    @Override
    public IAObject convertType(IAObject sourceObject, TypeCastingMathFunctionType mathFunction)
            throws HyracksDataException {
        if (mathFunction != TypeCastingMathFunctionType.NONE) {
            throw new RuntimeDataException(ErrorCode.INVALID_TYPE_CASTING_MATH_FUNCTION, mathFunction, ATypeTag.DOUBLE,
                    ATypeTag.FLOAT);
        }
        double sourceValue = ((ADouble) sourceObject).getDoubleValue();
        float targetValue = convert(sourceValue);
        return new AFloat(targetValue);
    }

    private float convert(double sourceValue) throws HyracksDataException {
        // Boundary check
        if (sourceValue > Float.MAX_VALUE) {
            if (strict) {
                raiseBoundaryCheckException(sourceValue);
            } else {
                return Float.MAX_VALUE;
            }
        } else if (sourceValue < -Float.MAX_VALUE) {
            if (strict) {
                raiseBoundaryCheckException(sourceValue);
            } else {
                return -Float.MAX_VALUE;
            }
        }

        return (float) sourceValue;
    }

    private void raiseBoundaryCheckException(double sourceValue) throws HyracksDataException {
        throw new RuntimeDataException(ErrorCode.TYPE_CONVERT_OUT_OF_BOUND, sourceValue, ATypeTag.FLOAT,
                Float.MAX_VALUE, -Float.MAX_VALUE);
    }
}
