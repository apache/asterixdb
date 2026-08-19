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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.network.ISocketChannel;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.exceptions.IPCException;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

final class IPCHandle implements IIPCHandle {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;

    private final IPCSystem system;

    private InetSocketAddress remoteAddress;

    private HandleState state;

    private SelectionKey key;

    private Object attachment;

    private int attachmentLen;

    private ByteBuffer inBuffer;

    private ByteBuffer outBuffer;

    private boolean full;

    private ISocketChannel socketChannel;

    IPCHandle(IPCSystem system, InetSocketAddress remoteAddress) {
        this.system = system;
        this.remoteAddress = remoteAddress;
        inBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        outBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        outBuffer.flip();
        state = HandleState.INITIAL;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    IPCSystem getIPCSystem() {
        return system;
    }

    @Override
    public long send(long requestId, Object req, Exception exception) throws IPCException {
        if (!isConnected()) {
            throw new IPCException("Handle is not in Connected state");
        }
        Message msg = new Message(this);
        long mid = system.createMessageId();
        msg.setMessageId(mid);
        msg.setRequestMessageId(requestId);
        if (exception != null) {
            msg.setFlag(Message.ERROR);
            msg.setPayload(exception);
        } else {
            msg.setFlag(Message.NORMAL);
            msg.setPayload(req);
        }
        system.getConnectionManager().send(msg);
        return mid;
    }

    @Override
    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }

    @Override
    public Object getAttachment() {
        return attachment;
    }

    @Override
    public int getAttachmentLen() {
        return attachmentLen;
    }

    SelectionKey getKey() {
        return key;
    }

    void setKey(SelectionKey key) {
        this.key = key;
    }

    public ISocketChannel getSocketChannel() {
        return socketChannel;
    }

    public void setSocketChannel(ISocketChannel socketChannel) {
        this.socketChannel = socketChannel;
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

    /**
     * Transitions to the given state unless the handle is already {@link HandleState#CLOSED}. A closed
     * handle stays closed: its waiter has already given up on it (e.g. {@link #waitTillConnected} timed
     * out and the caller retried with a fresh handle), so a late-completing connection must not resurrect
     * it into a live handle nobody references.
     *
     * @return true if the transition was made, false if the handle was already closed
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "closed is terminal")
    synchronized boolean setStateUnlessClosed(HandleState newState) {
        if (state == HandleState.CLOSED) {
            return false;
        }
        setState(newState);
        return true;
    }

    /**
     * Waits for this handle to reach a terminal state, for at most {@code timeoutMillis}. On expiry the
     * handle is marked {@link HandleState#CLOSED} so that it is treated as a failed connection and reaped
     * by {@link IPCConnectionManager#unregisterHandle}; teardown of its channel remains with the network
     * thread's existing close paths.
     *
     * @return true if the handle became connected, false if it was closed or the wait timed out
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "bound the connect wait")
    synchronized boolean waitTillConnected(long timeoutMillis) throws InterruptedException {
        final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            switch (state) {
                case INITIAL:
                case CONNECT_SENT:
                case CONNECT_RECEIVED:
                    final long remainingNanos = deadlineNanos - System.nanoTime();
                    if (remainingNanos <= 0) {
                        LOGGER.warn("timed out after {}ms waiting to connect to {} (state {})", timeoutMillis,
                                remoteAddress, state);
                        setState(HandleState.CLOSED);
                        return false;
                    }
                    TimeUnit.NANOSECONDS.timedWait(this, remainingNanos);
                    break;
                case CONNECTED:
                case CLOSED:
                    return state == HandleState.CONNECTED;
                default:
                    throw new IllegalStateException("unknown state: " + state);
            }
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
    }

    void processIncomingMessages() {
        inBuffer.flip();
        while (Message.hasMessage(inBuffer)) {
            Message message = new Message(this);
            try {
                message.read(inBuffer);
            } catch (Exception e) {
                message.setFlag(Message.ERROR);
                message.setPayload(e);
            }
            system.getPerformanceCounters().addMessageReceivedCount(1);

            final boolean error = message.getFlag() == Message.ERROR;
            if (!error && state == HandleState.CONNECT_RECEIVED) {
                remoteAddress = (InetSocketAddress) message.getPayload();
                setState(HandleState.CONNECTED);
                system.getConnectionManager().ack(this, message);
            } else if (!error && state == HandleState.CONNECT_SENT) {
                if (message.getFlag() == Message.INITIAL_ACK) {
                    setState(HandleState.CONNECTED);
                } else {
                    throw new IllegalStateException();
                }
            } else {
                attachmentLen = message.getPayloadLen();
                system.deliverIncomingMessage(message);
            }
        }
        inBuffer.compact();
    }

    void resizeInBuffer() {
        inBuffer.flip();
        ByteBuffer readBuffer = ByteBuffer.allocate(inBuffer.capacity() * 2);
        readBuffer.put(inBuffer);
        inBuffer = readBuffer;
    }

    void resizeOutBuffer() {
        ByteBuffer writeBuffer = ByteBuffer.allocate(outBuffer.capacity() * 2);
        writeBuffer.put(outBuffer);
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

    @Override
    public String toString() {
        return "IPCHandle [addr=" + remoteAddress + " state=" + state + "]";
    }
}
