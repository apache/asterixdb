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

class Message {
    private static final int MSG_SIZE_SIZE = 4;

    private static final int HEADER_SIZE = 17;

    static final byte INITIAL_REQ = 1;

    static final byte INITIAL_ACK = 2;

    static final byte ERROR = 3;

    static final byte NORMAL = 0;

    private IPCHandle ipcHandle;

    private long messageId;

    private long requestMessageId;

    private byte flag;

    private Object payload;

    Message(IPCHandle ipcHandle) {
        this.ipcHandle = ipcHandle;
    }

    IPCHandle getIPCHandle() {
        return ipcHandle;
    }

    void setMessageId(long messageId) {
        this.messageId = messageId;
    }

    long getMessageId() {
        return messageId;
    }

    void setRequestMessageId(long requestMessageId) {
        this.requestMessageId = requestMessageId;
    }

    long getRequestMessageId() {
        return requestMessageId;
    }

    void setFlag(byte flag) {
        this.flag = flag;
    }

    byte getFlag() {
        return flag;
    }

    void setPayload(Object payload) {
        this.payload = payload;
    }

    Object getPayload() {
        return payload;
    }

    static boolean hasMessage(ByteBuffer buffer) {
        if (buffer.remaining() < MSG_SIZE_SIZE) {
            return false;
        }
        int msgSize = buffer.getInt(buffer.position());
        return buffer.remaining() >= msgSize + MSG_SIZE_SIZE;
    }

    void read(ByteBuffer buffer) throws Exception {
        assert hasMessage(buffer);
        int msgSize = buffer.getInt();
        messageId = buffer.getLong();
        requestMessageId = buffer.getLong();
        flag = buffer.get();
        int finalPosition = buffer.position() + msgSize - HEADER_SIZE;
        int length = msgSize - HEADER_SIZE;
        try {
            IPayloadSerializerDeserializer serde = ipcHandle.getIPCSystem().getSerializerDeserializer();
            payload = flag == ERROR ? serde.deserializeException(buffer, length)
                    : serde.deserializeObject(buffer, length);
        } finally {
            buffer.position(finalPosition);
        }
    }

    boolean write(ByteBuffer buffer) throws Exception {
        IPayloadSerializerDeserializer serde = ipcHandle.getIPCSystem().getSerializerDeserializer();
        byte[] bytes = flag == ERROR ? serde.serializeException((Exception) payload) : serde.serializeObject(payload);
        if (buffer.remaining() >= MSG_SIZE_SIZE + HEADER_SIZE + bytes.length) {
            buffer.putInt(HEADER_SIZE + bytes.length);
            buffer.putLong(messageId);
            buffer.putLong(requestMessageId);
            buffer.put(flag);
            buffer.put(bytes);
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "MSG[" + messageId + ":" + requestMessageId + ":" + flag + ":" + payload + "]";
    }
}
