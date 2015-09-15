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

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.primitive.FloatPointable;

public class FloatToInt16TypeConvertComputer implements ITypeConvertComputer {

    public static final FloatToInt16TypeConvertComputer INSTANCE = new FloatToInt16TypeConvertComputer();

    private FloatToInt16TypeConvertComputer() {

    }

    @Override
    public void convertType(byte[] data, int start, int length, DataOutput out) throws IOException {
        float sourceValue = FloatPointable.getFloat(data, start);
        // Boundary check
        if (sourceValue > Short.MAX_VALUE || sourceValue < Short.MIN_VALUE) {
            throw new IOException("Cannot convert Float to INT16 - Float value " + sourceValue
                    + " is out of range that INT16 type can hold: INT16.MAX_VALUE:" + Short.MAX_VALUE
                    + ", INT16.MIN_VALUE: " + Short.MIN_VALUE);
        }
        // Math.floor to truncate decimal portion
        short targetValue = (short) Math.floor(sourceValue);
        out.writeByte(ATypeTag.INT16.serialize());
        out.writeShort(targetValue);
    }

}
