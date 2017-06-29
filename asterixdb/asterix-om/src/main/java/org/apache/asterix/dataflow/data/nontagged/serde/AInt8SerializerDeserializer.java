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

import org.apache.asterix.om.base.AInt8;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AInt8SerializerDeserializer implements ISerializerDeserializer<AInt8> {

    private static final long serialVersionUID = 1L;

    public static final AInt8SerializerDeserializer INSTANCE = new AInt8SerializerDeserializer();

    private AInt8SerializerDeserializer() {
    }

    @Override
    public AInt8 deserialize(DataInput in) throws HyracksDataException {
        try {
            return new AInt8(in.readByte());
        } catch (IOException ioe) {
            throw HyracksDataException.create(ioe);
        }
    }

    @Override
    public void serialize(AInt8 instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeByte(instance.getByteValue());
        } catch (IOException ioe) {
            throw HyracksDataException.create(ioe);
        }
    }

    public static byte getByte(byte[] bytes, int offset) {
        return bytes[offset];
    }
}
