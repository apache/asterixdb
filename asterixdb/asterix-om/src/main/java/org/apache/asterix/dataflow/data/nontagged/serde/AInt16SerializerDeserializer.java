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

import org.apache.asterix.om.base.AInt16;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.ShortSerializerDeserializer;

public class AInt16SerializerDeserializer implements ISerializerDeserializer<AInt16> {

    private static final long serialVersionUID = 1L;

    public static final AInt16SerializerDeserializer INSTANCE = new AInt16SerializerDeserializer();

    private AInt16SerializerDeserializer() {
    }

    @Override
    public AInt16 deserialize(DataInput in) throws HyracksDataException {
        return new AInt16(ShortSerializerDeserializer.read(in));
    }

    @Override
    public void serialize(AInt16 instance, DataOutput out) throws HyracksDataException {
        ShortSerializerDeserializer.write(instance.getShortValue(), out);
    }

    public static short getShort(byte[] bytes, int offset) {
        return (short) (((bytes[offset] & 0xff) << 8) + ((bytes[offset + 1] & 0xff) << 0));
    }

    public static int getUnsignedShort(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 8) + ((bytes[offset + 1] & 0xff) << 0);
    }
}
