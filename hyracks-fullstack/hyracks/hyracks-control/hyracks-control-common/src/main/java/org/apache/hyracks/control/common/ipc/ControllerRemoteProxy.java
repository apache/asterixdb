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
package org.apache.hyracks.control.common.ipc;

import java.net.InetSocketAddress;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.exceptions.IPCException;
import org.apache.hyracks.ipc.impl.IPCSystem;

public abstract class ControllerRemoteProxy {
    protected final IPCSystem ipc;
    private final InetSocketAddress inetSocketAddress;
    private final IControllerRemoteProxyIPCEventListener eventListener;
    private IIPCHandle ipcHandle;

    protected ControllerRemoteProxy(IPCSystem ipc, InetSocketAddress inetSocketAddress) {
        this(ipc, inetSocketAddress, null);
    }

    protected ControllerRemoteProxy(IPCSystem ipc, InetSocketAddress inetSocketAddress,
            IControllerRemoteProxyIPCEventListener eventListener) {
        this.ipc = ipc;
        this.inetSocketAddress = inetSocketAddress;
        this.eventListener = eventListener == null ? new IControllerRemoteProxyIPCEventListener() {
        } : eventListener;
    }

    protected IIPCHandle ensureIpcHandle() throws HyracksDataException {
        try {
            final boolean first = ipcHandle == null;
            if (first || !ipcHandle.isConnected()) {
                if (!first) {
                    getLogger().warning("ipcHandle " + ipcHandle + " disconnected; retrying connection");
                    eventListener.ipcHandleDisconnected(ipcHandle);
                }
                ipcHandle = ipc.getHandle(inetSocketAddress, getRetries(first));
                if (ipcHandle.isConnected()) {
                    if (first) {
                        eventListener.ipcHandleConnected(ipcHandle);
                    } else {
                        getLogger().warning("ipcHandle " + ipcHandle + " restored");
                        eventListener.ipcHandleRestored(ipcHandle);
                    }
                }
            }
        } catch (IPCException e) {
            throw HyracksDataException.create(e);
        }
        return ipcHandle;
    }

    protected abstract int getRetries(boolean first);

    protected abstract Logger getLogger();

    public InetSocketAddress getAddress() {
        return inetSocketAddress;
    }
}
