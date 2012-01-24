/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

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

    void read(ByteBuffer buffer) throws IOException, ClassNotFoundException {
        assert hasMessage(buffer);
        int msgSize = buffer.getInt();
        messageId = buffer.getLong();
        requestMessageId = buffer.getLong();
        flag = buffer.get();
        int finalPosition = buffer.position() + msgSize - HEADER_SIZE;
        try {
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(buffer.array(), buffer.position(),
                    msgSize - HEADER_SIZE));
            payload = ois.readObject();
            ois.close();
        } finally {
            buffer.position(finalPosition);
        }
    }

    boolean write(ByteBuffer buffer) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(payload);
        oos.close();
        byte[] bytes = baos.toByteArray();
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