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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.api.IExternalLangIPCProto;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.msgpack.core.MessagePack;

public class PythonDomainSocketProto extends AbstractPythonIPCProto implements IExternalLangIPCProto {
    private final String wd;
    SocketChannel chan;
    private ByteBuffer headerBuffer;
    private ProcessHandle pid;
    public static final int HYR_HEADER_SIZE = 21; // 4 (sz) + 8 (mid) + 8 (rmid) + 1 (flags)
    public static final int HYR_HEADER_SIZE_NOSZ = 17; // 8 + 8 + 1

    public PythonDomainSocketProto(OutputStream sockOut, SocketChannel chan, String wd) {
        super(sockOut);
        this.chan = chan;
        this.wd = wd;
        headerBuffer = ByteBuffer.allocate(HYR_HEADER_SIZE);
    }

    @Override
    public void start() {
    }

    @Override
    public void helo() throws IOException, AsterixException {
        recvBuffer.clear();
        recvBuffer.position(0);
        recvBuffer.limit(0);
        messageBuilder.reset();
        messageBuilder.helloDS(wd);
        sendHeader(routeId, messageBuilder.getLength());
        sendMsg(true);
        receiveMsg(true);
        byte pidType = recvBuffer.get();
        if (pidType != MessagePack.Code.UINT32 && pidType != MessagePack.Code.UINT16) {
            throw AsterixException.create(ErrorCode.EXTERNAL_UDF_EXCEPTION,
                    "Returned pid type is incorrect: " + pidType);
        }
        switch (pidType) {
            case MessagePack.Code.UINT32:
                pid = ProcessHandle.of(recvBuffer.getInt()).get();
                break;
            case MessagePack.Code.UINT16:
                pid = ProcessHandle.of(recvBuffer.getShort()).get();
                break;
            case MessagePack.Code.UINT8:
                pid = ProcessHandle.of(recvBuffer.get()).get();
                break;
            default:
                throw AsterixException.create(ErrorCode.EXTERNAL_UDF_EXCEPTION,
                        "Returned pid type is incorrect: " + pidType);
        }
        if (getResponseType() != MessageType.HELO) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "Expected HELO, recieved " + getResponseType().name());
        }
    }

    @Override
    public void sendMsg() throws IOException {
        sendMsg(false);
    }

    @Override
    public void sendMsg(ArrayBackedValueStorage args) throws IOException {
        sendMsg(false, args);
    }

    public void sendMsg(boolean sendIfDead) throws IOException {
        if (!sendIfDead && (pid == null || !pid.isAlive())) {
            return;
        }
        super.sendMsg();
    }

    public void sendMsg(boolean sendIfDead, ArrayBackedValueStorage args) throws IOException {
        if (!sendIfDead && (pid == null || !pid.isAlive())) {
            return;
        }
        super.sendMsg(args);
    }

    @Override
    public void receiveMsg() throws IOException, AsterixException {
        receiveMsg(false);
    }

    public void receiveMsg(boolean sendIfDead) throws IOException, AsterixException {
        if (!sendIfDead && (pid == null || !pid.isAlive())) {
            throw new AsterixException("Python process exited unexpectedly");
        }
        readFully(headerBuffer.capacity(), headerBuffer);
        if (headerBuffer.remaining() < Integer.BYTES) {
            recvBuffer.limit(0);
            throw new AsterixException("Python process exited unexpectedly");
        }
        int msgSz = headerBuffer.getInt() - HYR_HEADER_SIZE_NOSZ;
        if (recvBuffer.capacity() < msgSz) {
            recvBuffer = ByteBuffer.allocate(((msgSz / 32768) + 1) * 32768);
        }
        readFully(msgSz, recvBuffer);
        messageBuilder.readHead(recvBuffer);
        if (messageBuilder.type == MessageType.ERROR) {
            unpackerInput.reset(recvBuffer.array(), recvBuffer.position() + recvBuffer.arrayOffset(),
                    recvBuffer.remaining());
            unpacker.reset(unpackerInput);
            throw new AsterixException(unpacker.unpackString().replace('\0', ' '));
        }
    }

    private void readFully(int msgSz, ByteBuffer buf) throws IOException, AsterixException {
        buf.limit(msgSz);
        buf.clear();
        int read;
        int size = msgSz;
        while (size > 0) {
            read = chan.read(buf);
            if (read < 0) {
                throw new AsterixException("Socket closed");
            }
            size -= read;
        }
        buf.flip();
    }

    @Override
    public void quit() throws HyracksDataException {
        messageBuilder.quit();
    }

    public ProcessHandle getPid() {
        return pid;
    }

}
