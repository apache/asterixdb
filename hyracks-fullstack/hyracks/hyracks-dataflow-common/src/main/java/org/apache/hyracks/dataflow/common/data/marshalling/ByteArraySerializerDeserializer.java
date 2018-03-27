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
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;

public class ByteArraySerializerDeserializer implements ISerializerDeserializer<byte[]> {

    private static final long serialVersionUID = 1L;
    public static ByteArraySerializerDeserializer INSTANCE = new ByteArraySerializerDeserializer();

    private ByteArraySerializerDeserializer() {
    }

    /**
     * Return a pure byte array which doesn't have the length encoding prefix
     *
     * @param in
     *            - Stream to read instance from.
     * @return
     * @throws HyracksDataException
     */
    @Override
    public byte[] deserialize(DataInput in) throws HyracksDataException {
        return read(in);
    }

    /**
     * a pure content only byte array which doesn't have the encoded length at the beginning.
     * will write the entire array into the out
     */
    @Override
    public void serialize(byte[] instance, DataOutput out) throws HyracksDataException {
        write(instance, out);
    }

    public static byte[] read(DataInput in) throws HyracksDataException {
        try {
            int contentLength = VarLenIntEncoderDecoder.decode(in);
            byte[] bytes = new byte[contentLength];
            in.readFully(bytes, 0, contentLength);
            return bytes;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static void write(byte[] instance, DataOutput out) throws HyracksDataException {
        serialize(instance, 0, instance.length, out);
    }

    public void serialize(ByteArrayPointable byteArrayPtr, DataOutput out) throws HyracksDataException {
        try {
            out.write(byteArrayPtr.getByteArray(), byteArrayPtr.getStartOffset(), byteArrayPtr.getLength());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    // A pure byte array, which doesn't have the length information encoded at the beginning
    public static void serialize(byte[] instance, int start, int length, DataOutput out) throws HyracksDataException {
        byte[] metaBuffer = new byte[5];
        int metaLength = VarLenIntEncoderDecoder.encode(length, metaBuffer, 0);
        try {
            out.write(metaBuffer, 0, metaLength);
            out.write(instance, start, length);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

}
