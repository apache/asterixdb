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
package org.apache.hyracks.dataflow.common.data.marshalling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ShortSerializerDeserializer implements ISerializerDeserializer<Short> {
    private static final long serialVersionUID = 1L;

    public static final ShortSerializerDeserializer INSTANCE = new ShortSerializerDeserializer();

    private ShortSerializerDeserializer() {
    }

    @Override
    public Short deserialize(DataInput in) throws HyracksDataException {
        return read(in);
    }

    @Override
    public void serialize(Short instance, DataOutput out) throws HyracksDataException {
        write(instance, out);
    }

    public static short read(DataInput in) throws HyracksDataException {
        try {
            return in.readShort();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static void write(short instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeShort(instance);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
