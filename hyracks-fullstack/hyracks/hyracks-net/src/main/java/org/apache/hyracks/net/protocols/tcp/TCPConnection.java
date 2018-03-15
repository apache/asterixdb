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
package org.apache.hyracks.net.protocols.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TCPConnection {

    private static final Logger LOGGER = LogManager.getLogger();

    private final TCPEndpoint endpoint;

    private final SocketChannel channel;
    private final InetSocketAddress remoteAddress;
    private final SelectionKey key;

    private final Selector selector;

    private ITCPConnectionEventListener eventListener;

    private Object attachment;

    public TCPConnection(TCPEndpoint endpoint, SocketChannel channel, SelectionKey key, Selector selector) {
        this.endpoint = endpoint;
        this.channel = channel;
        this.key = key;
        this.selector = selector;
        remoteAddress = (InetSocketAddress) channel.socket().getRemoteSocketAddress();
    }

    public TCPEndpoint getEndpoint() {
        return endpoint;
    }

    public SocketChannel getSocketChannel() {
        return channel;
    }

    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.socket().getLocalSocketAddress();
    }

    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    public void enable(int ops) {
        key.interestOps(key.interestOps() | ops);
        selector.wakeup();
    }

    public void disable(int ops) {
        key.interestOps(key.interestOps() & ~(ops));
        selector.wakeup();
    }

    public ITCPConnectionEventListener getEventListener() {
        return eventListener;
    }

    public void setEventListener(ITCPConnectionEventListener eventListener) {
        this.eventListener = eventListener;
    }

    public Object getAttachment() {
        return attachment;
    }

    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }

    public synchronized void close() {
        key.cancel();
        try {
            channel.close();
        } catch (IOException e) {
            LOGGER.error(() -> "Error closing channel at: " + remoteAddress, e);
        }
    }

    @Override
    public String toString() {
        return "TCPConnection[Remote Address: " + remoteAddress + " Local Address: " + endpoint.getLocalAddress() + "]";
    }
}
