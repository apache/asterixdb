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
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;

public class IntegerToDoubleTypeConvertComputer implements ITypeConvertComputer {

    public static final IntegerToDoubleTypeConvertComputer INSTANCE = new IntegerToDoubleTypeConvertComputer();

    private IntegerToDoubleTypeConvertComputer() {

    }

    @Override
    public void convertType(byte[] data, int start, int length, DataOutput out) throws IOException {
        long val = 0L;

        // In order to convert a negative number correctly,
        // proper casting per INT type is needed.
        //
        switch (length) {
            case 1:
                // INT8
                val = (data[start] & 0xff);
                break;

            case 2:
                // INT16
                val = (short) ((data[start] << 8) | (data[start + 1] & 0xff));
                break;

            case 4:
                // INT32
                val = (int) (((data[start] & 0xff) << 24) | ((data[start + 1] & 0xff) << 16)
                        | ((data[start + 2] & 0xff) << 8) | (data[start + 3] & 0xff));
                break;

            case 8:
                // INT64
                val = (((long) (data[start] & 0xff) << 56) | ((long) (data[start + 1] & 0xff) << 48)
                        | ((long) (data[start + 2] & 0xff) << 40) | ((long) (data[start + 3] & 0xff) << 32)
                        | ((long) (data[start + 4] & 0xff) << 24) | ((long) (data[start + 5] & 0xff) << 16)
                        | ((long) (data[start + 6] & 0xff) << 8) | ((long) (data[start + 7] & 0xff)));

                break;

            default:
                break;
        }
        out.writeByte(ATypeTag.DOUBLE.serialize());
        DoubleSerializerDeserializer.INSTANCE.serialize(Double.valueOf(val), out);
    }

}
