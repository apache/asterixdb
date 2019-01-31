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
package org.apache.hyracks.ipc.impl;

import static org.apache.hyracks.api.util.JavaSerializationUtils.getSerializationProvider;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hyracks.ipc.api.IPayloadSerializerDeserializer;

public class JavaSerializationBasedPayloadSerializerDeserializer implements IPayloadSerializerDeserializer {

    @Override
    public Object deserializeObject(ByteBuffer buffer, int length) throws Exception {
        return deserialize(buffer, length);
    }

    @Override
    public Exception deserializeException(ByteBuffer buffer, int length) throws Exception {
        return (Exception) deserialize(buffer, length);
    }

    @Override
    public byte[] serializeObject(Object object) throws Exception {
        return serialize(object);
    }

    @Override
    public byte[] serializeException(Exception exception) throws Exception {
        return serialize(exception);
    }

    public static void serialize(OutputStream out, Object object) throws IOException {
        try (ObjectOutputStream oos = getSerializationProvider().newObjectOutputStream(out)) {
            oos.writeObject(object);
            oos.flush();
        }
    }

    private Object deserialize(ByteBuffer buffer, int length) throws Exception {
        Object object;
        try (ObjectInputStream ois = getSerializationProvider()
                .newObjectInputStream(new ByteArrayInputStream(buffer.array(), buffer.position(), length))) {
            object = ois.readObject();
        }
        return object;
    }

    private byte[] serialize(Object object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serialize(baos, object);
        baos.close();
        return baos.toByteArray();
    }
}
