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

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ByteArraySerializerDeserializer implements ISerializerDeserializer<byte[]> {

    private static final long serialVersionUID = 1L;

    public final static ByteArraySerializerDeserializer INSTANCE = new ByteArraySerializerDeserializer();

    private ByteArraySerializerDeserializer() {
    }

    @Override
    public byte[] deserialize(DataInput in) throws HyracksDataException {
        try {
            int length = in.readUnsignedShort();
            byte[] bytes = new byte[length + ByteArrayPointable.SIZE_OF_LENGTH];
            in.readFully(bytes, ByteArrayPointable.SIZE_OF_LENGTH, length);
            ByteArrayPointable.putLength(length, bytes, 0);
            return bytes;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(byte[] instance, DataOutput out) throws HyracksDataException {

        if (instance.length > ByteArrayPointable.MAX_LENGTH) {
            throw new HyracksDataException(
                    "encoded byte array too long: " + instance.length + " bytes");
        }
        try {
            int realLength = ByteArrayPointable.getFullLength(instance, 0);
            out.write(instance, 0, realLength);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public void serialize(byte[] instance, int start, int length, DataOutput out) throws HyracksDataException {
        if (length > ByteArrayPointable.MAX_LENGTH) {
            throw new HyracksDataException(
                    "encoded byte array too long: " + instance.length + " bytes");
        }
        try {
            out.write(instance, start, length);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

}
