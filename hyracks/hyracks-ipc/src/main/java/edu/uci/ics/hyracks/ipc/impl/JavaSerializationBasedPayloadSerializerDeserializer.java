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