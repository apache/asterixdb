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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.ipc.api.IIPCHandle;
import edu.uci.ics.hyracks.ipc.api.IResponseCallback;
import edu.uci.ics.hyracks.ipc.exceptions.IPCException;

final class IPCHandle implements IIPCHandle {
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;

    private final IPCSystem system;

    private InetSocketAddress remoteAddress;

    private final Map<Long, IResponseCallback> pendingRequestMap;

    private HandleState state;

    private SelectionKey key;

    private Object attachment;

    private ByteBuffer inBuffer;

    private ByteBuffer outBuffer;

    private boolean full;

    IPCHandle(IPCSystem system, InetSocketAddress remoteAddress) {
        this.system = system;
        this.remoteAddress = remoteAddress;
        pendingRequestMap = new HashMap<Long, IResponseCallback>();
        inBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        outBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        outBuffer.flip();
        state = HandleState.INITIAL;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    void setRemoteAddress(InetSocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    @Override
    public synchronized void send(Object req, IResponseCallback callback) throws IPCException {
        if (state != HandleState.CONNECTED) {
            throw new IPCException("Handle is not in Connected state");
        }
        Message msg = new Message(this);
        long mid = system.createMessageId();
        msg.setMessageId(mid);
        msg.setRequestMessageId(-1);
        msg.setPayload(req);
        if (callback != null) {
            pendingRequestMap.put(mid, callback);
        }
        system.getConnectionManager().write(msg);
    }

    @Override
    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }

    @Override
    public Object getAttachment() {
        return attachment;
    }

    SelectionKey getKey() {
        return key;
    }

    void setKey(SelectionKey key) {
        this.key = key;
    }

    public synchronized boolean isConnected() {
        return state == HandleState.CONNECTED;
    }

    synchronized HandleState getState() {
        return state;
    }

    synchronized void setState(HandleState state) {
        this.state = state;
        notifyAll();
    }
    
    synchronized void waitTillConnected() throws InterruptedException {
        while (!isConnected()) {
            wait();
        }
    }

    ByteBuffer getInBuffer() {
        return inBuffer;
    }

    ByteBuffer getOutBuffer() {
        return outBuffer;
    }

    synchronized void close() {
        setState(HandleState.CLOSED);
        for (IResponseCallback cb : pendingRequestMap.values()) {
            cb.callback(this, null, new IPCException("IPC Handle Closed"));
        }
    }

    synchronized void processIncomingMessages() {
        inBuffer.flip();
        while (Message.hasMessage(inBuffer)) {
            Message message = new Message(this);
            try {
                message.read(inBuffer);
            } catch (Exception e) {
                message.setFlag(Message.ERROR);
                message.setPayload(e);
            }

            if (state == HandleState.CONNECT_RECEIVED) {
                remoteAddress = (InetSocketAddress) message.getPayload();
                system.getConnectionManager().registerHandle(this);
                setState(HandleState.CONNECTED);
                system.getConnectionManager().ack(this, message);
                continue;
            } else if (state == HandleState.CONNECT_SENT) {
                if (message.getFlag() == Message.INITIAL_ACK) {
                    setState(HandleState.CONNECTED);
                } else {
                    throw new IllegalStateException();
                }
                continue;
            }
            long requestMessageId = message.getRequestMessageId();
            if (requestMessageId < 0) {
                system.deliverIncomingMessage(message);
            } else {
                Long rid = Long.valueOf(requestMessageId);
                IResponseCallback cb = pendingRequestMap.remove(rid);
                if (cb != null) {
                    byte flag = message.getFlag();
                    Object payload = flag == Message.ERROR ? null : message.getPayload();
                    Exception exception = (Exception) (flag == Message.ERROR ? message.getPayload() : null);
                    cb.callback(this, payload, exception);
                }
            }
        }
        inBuffer.compact();
    }

    void resizeInBuffer() {
        inBuffer.flip();
        ByteBuffer readBuffer = ByteBuffer.allocate(inBuffer.capacity() * 2);
        readBuffer.put(inBuffer);
        readBuffer.compact();
        inBuffer = readBuffer;
    }

    void resizeOutBuffer() {
        ByteBuffer writeBuffer = ByteBuffer.allocate(outBuffer.capacity() * 2);
        writeBuffer.put(outBuffer);
        writeBuffer.compact();
        writeBuffer.flip();
        outBuffer = writeBuffer;
    }

    void markFull() {
        full = true;
    }

    void clearFull() {
        full = false;
    }

    boolean full() {
        return full;
    }
}