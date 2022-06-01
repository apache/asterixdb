/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.ipc;

import static org.apache.hyracks.api.util.JavaSerializationUtils.getSerializationProvider;
import static org.msgpack.core.MessagePack.Code.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.apache.asterix.external.library.msgpack.MessagePackUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class PythonMessageBuilder {
    MessageType type;
    long dataLength;
    private final ByteBuffer buf;

    public PythonMessageBuilder() {
        this.type = null;
        dataLength = -1;
        this.buf = ByteBuffer.allocate(4096);
    }

    public void reset() {
        //TODO: should be able to get away w/o clearing buf?
        buf.clear();
    }

    public ByteBuffer getBuf() {
        return buf;
    }

    public int getLength() {
        return buf.position() - buf.arrayOffset();
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public void packHeader() throws HyracksDataException {
        MessagePackUtils.packFixPos(buf, (byte) type.ordinal());
    }

    //TODO: this is wrong for any multibyte chars
    private static int getStringLength(String s) {
        return s.length();
    }

    public void readHead(ByteBuffer buf) {
        byte typ = buf.get();
        type = MessageType.fromByte(typ);
    }

    public void hello() throws IOException {
        this.type = MessageType.HELO;
        byte[] serAddr = serialize(new InetSocketAddress(InetAddress.getLoopbackAddress(), 1));
        dataLength = serAddr.length + 1;
        packHeader();
        //TODO:make this cleaner
        buf.put(BIN32);
        buf.putInt(serAddr.length);
        buf.put(serAddr);
    }

    public void helloDS(String modulePath) throws IOException {
        this.type = MessageType.HELO;
        // sum(string lengths) + 2 from fix array tag and message type
        dataLength = PythonMessageBuilder.getStringLength(modulePath) + 2;
        packHeader();
        MessagePackUtils.packFixArrayHeader(buf, (byte) 2);
        MessagePackUtils.packStr(buf, "HELLO");
        MessagePackUtils.packStr(buf, modulePath);
    }

    public void quit() throws HyracksDataException {
        this.type = MessageType.QUIT;
        dataLength = getStringLength("QUIT");
        packHeader();
        MessagePackUtils.packFixStr(buf, "QUIT");
    }

    public void init(final String module, final String clazz, final String fn) throws HyracksDataException {
        this.type = MessageType.INIT;
        // sum(string lengths) + 2 from fix array tag and message type
        if (clazz != null) {
            dataLength =
                    PythonMessageBuilder.getStringLength(module) + getStringLength(clazz) + getStringLength(fn) + 2;
        } else {
            dataLength = PythonMessageBuilder.getStringLength(module) + getStringLength(fn) + 2;
        }
        packHeader();
        int numArgs = clazz == null ? 2 : 3;
        MessagePackUtils.packFixArrayHeader(buf, (byte) numArgs);
        MessagePackUtils.packStr(buf, module);
        if (clazz != null) {
            MessagePackUtils.packStr(buf, clazz);
        }
        MessagePackUtils.packStr(buf, fn);
    }

    public void call(int numArgs, int len) throws HyracksDataException {
        buf.clear();
        buf.position(0);
        this.type = MessageType.CALL;
        dataLength = 5 + 1 + len;
        packHeader();
        //TODO: make this switch between fixarray/array16/array32
        buf.put((byte) (FIXARRAY_PREFIX + 1));
        buf.put(ARRAY16);
        buf.putShort((short) numArgs);
    }

    public void callMulti(int lim, int numArgs) throws HyracksDataException {
        buf.clear();
        buf.position(0);
        this.type = MessageType.CALL;
        dataLength = 5 + 1 + lim;
        packHeader();
        //TODO: make this switch between fixarray/array16/array32
        buf.put(ARRAY16);
        buf.putShort((short) numArgs);
    }

    //this is used to send a serialized java inetaddress to the entrypoint so it can send it back
    //to the IPC subsystem, which needs it. don't use this for anything else.
    private byte[] serialize(Object object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = getSerializationProvider().newObjectOutputStream(baos)) {
            oos.writeObject(object);
            oos.flush();
            baos.close();
        }
        return baos.toByteArray();
    }
}
