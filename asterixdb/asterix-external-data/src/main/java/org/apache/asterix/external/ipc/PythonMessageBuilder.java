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

import org.apache.asterix.external.library.msgpack.MessagePackerFromADM;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class PythonMessageBuilder {
    private static final int MAX_BUF_SIZE = 64 * 1024 * 1024; //64MB.
    MessageType type;
    long dataLength;
    ByteBuffer buf;

    public PythonMessageBuilder() {
        this.type = null;
        dataLength = -1;
        this.buf = ByteBuffer.allocate(4096);
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public void packHeader() throws HyracksDataException {
        MessagePackerFromADM.packFixPos(buf, (byte) type.ordinal());
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

    public void quit() throws HyracksDataException {
        this.type = MessageType.QUIT;
        dataLength = getStringLength("QUIT");
        packHeader();
        MessagePackerFromADM.packFixStr(buf, "QUIT");
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
        MessagePackerFromADM.packFixArrayHeader(buf, (byte) numArgs);
        MessagePackerFromADM.packStr(buf, module);
        if (clazz != null) {
            MessagePackerFromADM.packStr(buf, clazz);
        }
        MessagePackerFromADM.packStr(buf, fn);
    }

    public void call(byte[] args, int lim, int numArgs) throws HyracksDataException {
        if (args.length > buf.capacity()) {
            int growTo = ExternalFunctionResultRouter.closestPow2(args.length);
            if (growTo > MAX_BUF_SIZE) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                        "Unable to allocate message buffer larger than:" + MAX_BUF_SIZE + " bytes");
            }
            buf = ByteBuffer.allocate(growTo);
        }
        buf.clear();
        buf.position(0);
        this.type = MessageType.CALL;
        dataLength = 5 + 1 + lim;
        packHeader();
        //TODO: make this switch between fixarray/array16/array32
        buf.put((byte) (FIXARRAY_PREFIX + 1));
        buf.put(ARRAY32);
        buf.putInt(numArgs);
        if (numArgs > 0) {
            buf.put(args, 0, lim);
        }
    }

    public void callMulti(byte[] args, int lim, int numArgs) throws HyracksDataException {
        if (args.length > buf.capacity()) {
            int growTo = ExternalFunctionResultRouter.closestPow2(args.length);
            if (growTo > MAX_BUF_SIZE) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                        "Unable to allocate message buffer larger than:" + MAX_BUF_SIZE + " bytes");
            }
            buf = ByteBuffer.allocate(growTo);
        }
        buf.clear();
        buf.position(0);
        this.type = MessageType.CALL;
        dataLength = 5 + 1 + lim;
        packHeader();
        //TODO: make this switch between fixarray/array16/array32
        buf.put(ARRAY16);
        buf.putShort((short) numArgs);
        if (numArgs > 0) {
            buf.put(args, 0, lim);
        }
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
