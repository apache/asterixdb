/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.asterix.om.types.hierachy;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.primitive.DoublePointable;

public class DoubleToFloatTypeConvertComputer implements ITypeConvertComputer {

    public static final DoubleToFloatTypeConvertComputer INSTANCE = new DoubleToFloatTypeConvertComputer();

    private DoubleToFloatTypeConvertComputer() {

    }

    @Override
    public void convertType(byte[] data, int start, int length, DataOutput out) throws IOException {
        double sourceValue = DoublePointable.getDouble(data, start);
        // Boundary check
        if (sourceValue > Float.MAX_VALUE || sourceValue < Float.MIN_VALUE) {
            throw new IOException("Cannot convert Double to Float - Double value " + sourceValue
                    + " is out of range that Float type can hold: Float.MAX_VALUE:" + Float.MAX_VALUE
                    + ", Float.MIN_VALUE: " + Float.MIN_VALUE);
        }
        out.writeByte(ATypeTag.FLOAT.serialize());
        out.writeFloat((float) sourceValue);
    }

}
