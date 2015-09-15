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
import org.apache.hyracks.data.std.primitive.DoublePointable;

public class DoubleToInt64TypeConvertComputer implements ITypeConvertComputer {

    public static final DoubleToInt64TypeConvertComputer INSTANCE = new DoubleToInt64TypeConvertComputer();

    private DoubleToInt64TypeConvertComputer() {

    }

    @Override
    public void convertType(byte[] data, int start, int length, DataOutput out) throws IOException {
        double sourceValue = DoublePointable.getDouble(data, start);
        // Boundary check
        if (sourceValue > Long.MAX_VALUE || sourceValue < Long.MIN_VALUE) {
            throw new IOException("Cannot convert Double to INT64 - Double value " + sourceValue
                    + " is out of range that INT64 type can hold: INT64.MAX_VALUE:" + Long.MAX_VALUE
                    + ", INT64.MIN_VALUE: " + Long.MIN_VALUE);
        }
        // Math.floor to truncate decimal portion
        long targetValue = (long) Math.floor(sourceValue);
        out.writeByte(ATypeTag.INT64.serialize());
        out.writeLong(targetValue);
    }

}
