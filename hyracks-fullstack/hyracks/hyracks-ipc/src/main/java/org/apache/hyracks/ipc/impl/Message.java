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

import java.nio.ByteBuffer;

import org.apache.hyracks.ipc.api.IPayloadSerializerDeserializer;

public class Message {
    private static final int MSG_SIZE_SIZE = 4;

    public static final int HEADER_SIZE = 17;

    public static final byte INITIAL_REQ = 1;

    public static final byte INITIAL_ACK = 2;

    public static final byte ERROR = 3;

    public static final byte NORMAL = 0;

    private IPCHandle ipcHandle;

    private long messageId;

    private long requestMessageId;

    private byte flag;

    private Object payload;

    private int payloadLen;

    public Message(IPCHandle ipcHandle) {
        this.ipcHandle = ipcHandle;
    }

    IPCHandle getIPCHandle() {
        return ipcHandle;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }

    public long getMessageId() {
        return messageId;
    }

    public void setRequestMessageId(long requestMessageId) {
        this.requestMessageId = requestMessageId;
    }

    public long getRequestMessageId() {
        return requestMessageId;
    }

    public void setFlag(byte flag) {
        this.flag = flag;
    }

    public byte getFlag() {
        return flag;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    Object getPayload() {
        return payload;
    }

    int getPayloadLen() {
        return payloadLen;
    }

    public static boolean hasMessage(ByteBuffer buffer) {
        if (buffer.remaining() < MSG_SIZE_SIZE) {
            return false;
        }
        int msgSize = buffer.getInt(buffer.position());
        return msgSize > 0 && buffer.remaining() >= msgSize + MSG_SIZE_SIZE;
    }

    public void read(ByteBuffer buffer) throws Exception {
        assert hasMessage(buffer);
        int msgSize = buffer.getInt();
        messageId = buffer.getLong();
        requestMessageId = buffer.getLong();
        flag = buffer.get();
        int finalPosition = buffer.position() + msgSize - HEADER_SIZE;
        int length = msgSize - HEADER_SIZE;
        payloadLen = length;
        try {
            IPayloadSerializerDeserializer serde = ipcHandle.getIPCSystem().getSerializerDeserializer();
            switch (flag) {
                case NORMAL:
                case INITIAL_ACK:
                case INITIAL_REQ:
                    payload = serde.deserializeObject(buffer, length, flag);
                    break;
                case ERROR:
                    payload = serde.deserializeException(buffer, length);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown message flag");
            }

        } finally {
            buffer.position(finalPosition);
        }
    }

    public boolean write(ByteBuffer buffer) throws Exception {
        IPayloadSerializerDeserializer serde = ipcHandle.getIPCSystem().getSerializerDeserializer();
        return write(buffer, serde);
    }

    public boolean write(ByteBuffer buffer, IPayloadSerializerDeserializer serde) throws Exception {
        byte[] bytes = flag == ERROR ? serde.serializeException((Exception) payload) : serde.serializeObject(payload);
        if (buffer.remaining() >= MSG_SIZE_SIZE + HEADER_SIZE + bytes.length) {
            writeHeader(buffer, bytes.length, messageId, requestMessageId, flag);
            buffer.put(bytes);
            return true;
        }
        return false;
    }

    public static void writeHeader(ByteBuffer buffer, int dlen, long messageId, long requestMessageId, byte flag) {
        buffer.putInt(HEADER_SIZE + dlen);
        buffer.putLong(messageId);
        buffer.putLong(requestMessageId);
        buffer.put(flag);
    }

    @Override
    public String toString() {
        return "MSG[" + messageId + ":" + requestMessageId + ":" + flag + ":" + payload + "]";
    }
}
