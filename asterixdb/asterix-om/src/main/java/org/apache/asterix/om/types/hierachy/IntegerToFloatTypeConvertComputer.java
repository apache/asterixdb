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
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy.TypeCastingMathFunctionType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;

public class IntegerToFloatTypeConvertComputer implements ITypeConvertComputer {

    private static final IntegerToFloatTypeConvertComputer INSTANCE = new IntegerToFloatTypeConvertComputer();

    private IntegerToFloatTypeConvertComputer() {
    }

    public static IntegerToFloatTypeConvertComputer getInstance() {
        return INSTANCE;
    }

    @Override
    public void convertType(byte[] data, int start, int length, DataOutput out) throws IOException {
        float val;
        // In order to convert a negative number correctly, proper casting per INT type is needed.
        switch (length) {
            case 1: // TINYINT
                val = BytePointable.getByte(data, start);
                break;
            case 2: // SMALLINT
                val = ShortPointable.getShort(data, start);
                break;
            case 4: // INTEGER
                val = IntegerPointable.getInteger(data, start);
                break;
            case 8: // BIGINT
                val = LongPointable.getLong(data, start);
                break;
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_CONVERT_INTEGER_SOURCE, Arrays.toString(
                        new ATypeTag[] { ATypeTag.TINYINT, ATypeTag.SMALLINT, ATypeTag.INTEGER, ATypeTag.BIGINT }));
        }
        out.writeByte(ATypeTag.FLOAT.serialize());
        FloatSerializerDeserializer.write(val, out);
    }

    @Override
    public IAObject convertType(IAObject sourceObject, TypeCastingMathFunctionType mathFunction)
            throws HyracksDataException {
        float val;
        switch (sourceObject.getType().getTypeTag()) {
            case TINYINT:
                val = ((AInt8) sourceObject).getByteValue();
                break;
            case SMALLINT:
                val = ((AInt16) sourceObject).getShortValue();
                break;
            case INTEGER:
                val = ((AInt32) sourceObject).getIntegerValue();
                break;
            case BIGINT:
                val = ((AInt64) sourceObject).getLongValue();
                break;
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_CONVERT_INTEGER_SOURCE, Arrays.toString(
                        new ATypeTag[] { ATypeTag.TINYINT, ATypeTag.SMALLINT, ATypeTag.INTEGER, ATypeTag.BIGINT }));
        }
        return new AFloat(val);
    }
}
