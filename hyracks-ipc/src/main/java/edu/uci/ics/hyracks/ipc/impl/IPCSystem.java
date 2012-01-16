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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import edu.uci.ics.hyracks.ipc.api.IIPCHandle;
import edu.uci.ics.hyracks.ipc.api.IIPCI;
import edu.uci.ics.hyracks.ipc.exceptions.IPCException;

public class IPCSystem {
    private final IPCConnectionManager cMgr;

    private final IIPCI ipci;

    private final Executor executor;

    private final AtomicLong midFactory;

    public IPCSystem(InetSocketAddress socketAddress) throws IOException {
        this(socketAddress, null, null);
    }

    public IPCSystem(InetSocketAddress socketAddress, IIPCI ipci, Executor executor) throws IOException {
        cMgr = new IPCConnectionManager(this, socketAddress);
        this.ipci = ipci;
        this.executor = executor;
        midFactory = new AtomicLong();
    }

    public InetSocketAddress getSocketAddress() {
        return cMgr.getAddress();
    }

    public void start() {
        cMgr.start();
    }

    public IIPCHandle getHandle(InetSocketAddress remoteAddress) throws IPCException {
        try {
            return cMgr.getIPCHandle(remoteAddress);
        } catch (IOException e) {
            throw new IPCException(e);
        } catch (InterruptedException e) {
            throw new IPCException(e);
        }
    }

    long createMessageId() {
        return midFactory.incrementAndGet();
    }

    void deliverIncomingMessage(final Message message) {
        assert message.getFlag() == Message.NORMAL;
        executor.execute(new Runnable() {
            @Override
            public void run() {
                IPCHandle handle = message.getIPCHandle();
                Message response = new Message(handle);
                response.setMessageId(createMessageId());
                response.setRequestMessageId(message.getMessageId());
                response.setFlag(Message.NORMAL);
                try {
                    Object result = ipci.call(handle, message.getPayload());
                    response.setPayload(result);
                } catch (Exception e) {
                    response.setFlag(Message.ERROR);
                    response.setPayload(e);
                }
                cMgr.write(response);
            }
        });
    }

    IPCConnectionManager getConnectionManager() {
        return cMgr;
    }
}