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
package org.apache.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public final class SerializerDeserializerUtil {

    public static void writeIntToByteArray(byte[] array, int value, int offset) {
        array[offset] = (byte) (0xff & (value >> 24));
        array[offset + 1] = (byte) (0xff & (value >> 16));
        array[offset + 2] = (byte) (0xff & (value >> 8));
        array[offset + 3] = (byte) (0xff & value);
    }

    public static void writeShortToByteArray(byte[] array, short value, int offset) {
        array[offset] = (byte) (0xff & (value >> 8));
        array[offset + 1] = (byte) (0xff & value);
    }

    public static int readIntFromByteArray(byte[] array) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            int shift = (3 - i) * 8;
            value += (array[i] & 0x000000FF) << shift;
        }
        return value;
    }

    public static short readShortFromByteArray(byte[] array) {
        short value = 0;
        for (int i = 0; i < 2; i++) {
            int shift = (1 - i) * 8;
            value += (array[i] & 0x000000FF) << shift;
        }
        return value;
    }

    public static void serializeTag(IAObject instance, DataOutput out) throws HyracksDataException {
        IAType t = instance.getType();
        ATypeTag tag = t.getTypeTag();
        try {
            out.writeByte(tag.serialize());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static ATypeTag deserializeTag(DataInput in) throws HyracksDataException {
        try {
            return EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(in.readByte());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }
}
