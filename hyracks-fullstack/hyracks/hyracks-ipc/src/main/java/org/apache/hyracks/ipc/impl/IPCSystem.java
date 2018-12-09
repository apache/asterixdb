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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hyracks.api.network.ISocketChannelFactory;
import org.apache.hyracks.ipc.api.IIPCEventListener;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.api.IIPCI;
import org.apache.hyracks.ipc.api.IPCPerformanceCounters;
import org.apache.hyracks.ipc.api.IPayloadSerializerDeserializer;
import org.apache.hyracks.ipc.exceptions.IPCException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IPCSystem {
    private static final Logger LOGGER = LogManager.getLogger();

    private final IPCConnectionManager cMgr;

    private final IIPCI ipci;

    private final IPayloadSerializerDeserializer serde;

    private final AtomicLong midFactory;

    private final IPCPerformanceCounters perfCounters;

    public IPCSystem(InetSocketAddress socketAddress, ISocketChannelFactory socketChannelFactory, IIPCI ipci,
            IPayloadSerializerDeserializer serde) throws IOException {
        cMgr = new IPCConnectionManager(this, socketAddress, socketChannelFactory);
        this.ipci = ipci;
        this.serde = serde;
        midFactory = new AtomicLong();
        perfCounters = new IPCPerformanceCounters();
    }

    public InetSocketAddress getSocketAddress() {
        return cMgr.getAddress();
    }

    public void start() {
        cMgr.start();
    }

    public void stop() {
        cMgr.stop();
    }

    public IIPCHandle getHandle(InetSocketAddress remoteAddress, int maxRetries) throws IPCException {
        return getHandle(remoteAddress, maxRetries, 0);
    }

    public IIPCHandle getReconnectingHandle(InetSocketAddress remoteAddress) throws IPCException {
        return getReconnectingHandle(remoteAddress, 1);
    }

    public IIPCHandle getHandle(InetSocketAddress remoteAddress, int maxRetries, int reconnectAttempts,
            IIPCEventListener eventListener) throws IPCException {
        if (reconnectAttempts > 0) {
            return new ReconnectingIPCHandle(this, eventListener, remoteAddress, maxRetries, reconnectAttempts);
        }
        try {
            return cMgr.getIPCHandle(remoteAddress, maxRetries);
        } catch (IOException e) {
            throw new IPCException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IPCException(e);
        }
    }

    IPayloadSerializerDeserializer getSerializerDeserializer() {
        return serde;
    }

    long createMessageId() {
        return midFactory.incrementAndGet();
    }

    void deliverIncomingMessage(final Message message) {
        long mid = message.getMessageId();
        long rmid = message.getRequestMessageId();
        Object payload = null;
        Exception exception = null;
        if (message.getFlag() == Message.ERROR) {
            exception = (Exception) message.getPayload();
            LOGGER.log(Level.INFO, "Exception in message", exception);
        } else {
            payload = message.getPayload();
        }
        ipci.deliverIncomingMessage(message.getIPCHandle(), mid, rmid, payload, exception);
    }

    IPCConnectionManager getConnectionManager() {
        return cMgr;
    }

    public IPCPerformanceCounters getPerformanceCounters() {
        return perfCounters;
    }

    private IIPCHandle getReconnectingHandle(InetSocketAddress remoteAddress, int reconnectAttempts)
            throws IPCException {
        return getHandle(remoteAddress, 0, reconnectAttempts, NoOpIPCEventListener.INSTANCE);
    }

    private IIPCHandle getHandle(InetSocketAddress remoteAddress, int maxRetries, int reconnectAttempts)
            throws IPCException {
        return getHandle(remoteAddress, maxRetries, reconnectAttempts, NoOpIPCEventListener.INSTANCE);
    }
}
