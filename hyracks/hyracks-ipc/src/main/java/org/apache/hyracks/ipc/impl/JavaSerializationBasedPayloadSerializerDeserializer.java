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
package edu.uci.ics.hyracks.ipc.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.ipc.api.IPayloadSerializerDeserializer;

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

    public static void serialize(OutputStream out, Object object) throws Exception {
        ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(object);
        oos.flush();
    }

    private Object deserialize(ByteBuffer buffer, int length) throws Exception {
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(buffer.array(), buffer.position(),
                length));
        Object object = ois.readObject();
        ois.close();
        return object;
    }

    private byte[] serialize(Object object) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serialize(baos, object);
        baos.close();
        return baos.toByteArray();
    }
}