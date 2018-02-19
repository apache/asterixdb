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

import org.apache.hyracks.ipc.api.IIPCEventListener;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.exceptions.IPCException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class ReconnectingIPCHandle implements IIPCHandle {

    private static final Logger LOGGER = LogManager.getLogger();
    private final IPCSystem ipc;
    private final int reconnectAttempts;
    private final IIPCEventListener listener;
    private IIPCHandle delegate;

    ReconnectingIPCHandle(IPCSystem ipc, IIPCEventListener listener, InetSocketAddress remoteAddress, int maxRetries,
            int reconnectAttempts) throws IPCException {
        this.ipc = ipc;
        this.listener = listener;
        this.reconnectAttempts = reconnectAttempts;
        this.delegate = ipc.getHandle(remoteAddress, maxRetries);
        listener.ipcHandleConnected(delegate);
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return delegate.getRemoteAddress();
    }

    @Override
    public long send(long requestId, Object payload, Exception exception) throws IPCException {
        return ensureConnected().send(requestId, payload, exception);
    }

    @Override
    public void setAttachment(Object attachment) {
        delegate.setAttachment(attachment);
    }

    @Override
    public Object getAttachment() {
        return delegate.getAttachment();
    }

    @Override
    public boolean isConnected() {
        return delegate.isConnected();
    }

    private IIPCHandle ensureConnected() throws IPCException {
        if (delegate.isConnected()) {
            return delegate;
        }
        synchronized (this) {
            if (delegate.isConnected()) {
                return delegate;
            }
            LOGGER.warn("ipcHandle {} disconnected; will attempt to reconnect {} times", delegate, reconnectAttempts);
            listener.ipcHandleDisconnected(delegate);
            delegate = ipc.getHandle(getRemoteAddress(), reconnectAttempts);
            LOGGER.warn("ipcHandle {} restored", delegate);
            listener.ipcHandleRestored(delegate);
            return delegate;
        }
    }

}
