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
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;

public class FloatToDoubleTypeConvertComputer implements ITypeConvertComputer {

    private static final FloatToDoubleTypeConvertComputer INSTANCE = new FloatToDoubleTypeConvertComputer();

    private FloatToDoubleTypeConvertComputer() {
    }

    public static FloatToDoubleTypeConvertComputer getInstance() {
        return INSTANCE;
    }

    @Override
    public void convertType(byte[] data, int start, int length, DataOutput out) throws IOException {
        double targetValue = convertType(data, start);
        out.writeByte(ATypeTag.DOUBLE.serialize());
        DoubleSerializerDeserializer.write(targetValue, out);
    }

    @Override
    public IAObject convertType(IAObject sourceObject, TypeCastingMathFunctionType mathFunction)
            throws HyracksDataException {
        if (mathFunction != TypeCastingMathFunctionType.NONE) {
            throw new RuntimeDataException(ErrorCode.INVALID_TYPE_CASTING_MATH_FUNCTION, mathFunction, ATypeTag.FLOAT,
                    ATypeTag.DOUBLE);
        }
        double targetValue = ((AFloat) sourceObject).getFloatValue();
        return new ADouble(targetValue);
    }

    double convertType(byte[] data, int start) {
        return FloatPointable.getFloat(data, start);
    }
}
