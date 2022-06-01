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
package org.apache.asterix.external.ipc;

import static org.apache.hyracks.ipc.impl.Message.HEADER_SIZE;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.api.IExternalLangIPCProto;
import org.apache.asterix.external.library.msgpack.MsgPackPointableVisitor;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.ipc.impl.Message;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.ArrayBufferInput;

public abstract class AbstractPythonIPCProto {
    public static final int HEADER_SIZE_LEN_INCLUSIVE = 21;
    protected final PythonMessageBuilder messageBuilder;
    protected final DataOutputStream sockOut;
    protected final ArrayBufferInput unpackerInput;
    protected final MessageUnpacker unpacker;
    protected final ArrayBackedValueStorage argsStorage;
    protected final PointableAllocator pointableAllocator;
    protected final MsgPackPointableVisitor pointableVisitor;
    private final ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE_LEN_INCLUSIVE);
    protected ByteBuffer recvBuffer = ByteBuffer.allocate(32768);
    protected long routeId;
    protected Pair<ByteBuffer, Exception> bufferBox;
    protected long maxFunctionId;

    public AbstractPythonIPCProto(OutputStream sockOut) {
        messageBuilder = new PythonMessageBuilder();
        this.sockOut = new DataOutputStream(sockOut);
        this.maxFunctionId = 0L;
        unpackerInput = new ArrayBufferInput(new byte[0]);
        unpacker = MessagePack.newDefaultUnpacker(unpackerInput);
        this.argsStorage = new ArrayBackedValueStorage();
        this.pointableAllocator = new PointableAllocator();
        this.pointableVisitor = new MsgPackPointableVisitor();
    }

    public void helo() throws IOException, AsterixException {
        recvBuffer.clear();
        recvBuffer.position(0);
        recvBuffer.limit(0);
        messageBuilder.reset();
        messageBuilder.hello();
        sendHeader(routeId, messageBuilder.getLength());
        sendMsg();
        receiveMsg();
        if (getResponseType() != MessageType.HELO) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "Expected HELO, recieved " + getResponseType().name());
        }
    }

    public long init(String module, String clazz, String fn) throws IOException, AsterixException {
        long functionId = maxFunctionId++;
        recvBuffer.clear();
        recvBuffer.position(0);
        recvBuffer.limit(0);
        messageBuilder.reset();
        messageBuilder.init(module, clazz, fn);
        sendHeader(functionId, messageBuilder.getLength());
        sendMsg();
        receiveMsg();
        if (getResponseType() != MessageType.INIT_RSP) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "Expected INIT_RSP, recieved " + getResponseType().name());
        }
        return functionId;
    }

    public ByteBuffer call(long functionId, IAType[] argTypes, IValueReference[] argValues, boolean nullCall)
            throws IOException, AsterixException {
        recvBuffer.clear();
        recvBuffer.position(0);
        recvBuffer.limit(0);
        messageBuilder.reset();
        argsStorage.reset();
        for (int i = 0; i < argTypes.length; i++) {
            IExternalLangIPCProto.visitValueRef(argTypes[i], argsStorage.getDataOutput(), argValues[i],
                    pointableAllocator, pointableVisitor, nullCall);
        }
        int len = argsStorage.getLength() + 5;
        sendHeader(functionId, len);
        messageBuilder.call(argValues.length, len);
        sendMsg(argsStorage);
        receiveMsg();
        if (getResponseType() != MessageType.CALL_RSP) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "Expected CALL_RSP, recieved " + getResponseType().name());
        }
        return recvBuffer;
    }

    public ByteBuffer callMulti(long key, ArrayBackedValueStorage args, int numTuples)
            throws IOException, AsterixException {
        recvBuffer.clear();
        recvBuffer.position(0);
        recvBuffer.limit(0);
        messageBuilder.reset();
        int len = args.getLength() + 4;
        sendHeader(key, len);
        messageBuilder.callMulti(0, numTuples);
        sendMsg(args);
        receiveMsg();
        if (getResponseType() != MessageType.CALL_RSP) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "Expected CALL_RSP, recieved " + getResponseType().name());
        }
        return recvBuffer;
    }

    public void quit() throws HyracksDataException {
        messageBuilder.quit();
    }

    public abstract void receiveMsg() throws IOException, AsterixException;

    public void sendHeader(long key, int msgLen) throws IOException {
        headerBuffer.clear();
        headerBuffer.position(0);
        headerBuffer.putInt(HEADER_SIZE + Integer.BYTES + msgLen);
        headerBuffer.putLong(key);
        headerBuffer.putLong(routeId);
        headerBuffer.put(Message.NORMAL);
        sockOut.write(headerBuffer.array(), 0, HEADER_SIZE + Integer.BYTES);
        sockOut.flush();
    }

    public void sendMsg(ArrayBackedValueStorage content) throws IOException {
        sockOut.write(messageBuilder.getBuf().array(), messageBuilder.getBuf().arrayOffset(),
                messageBuilder.getBuf().position());
        sockOut.write(content.getByteArray(), content.getStartOffset(), content.getLength());
        sockOut.flush();
    }

    public void sendMsg() throws IOException {
        sockOut.write(messageBuilder.getBuf().array(), messageBuilder.getBuf().arrayOffset(),
                messageBuilder.getBuf().position());
        sockOut.flush();
    }

    public MessageType getResponseType() {
        return messageBuilder.type;
    }

    public long getRouteId() {
        return routeId;
    }

    public DataOutputStream getSockOut() {
        return sockOut;
    }
}
