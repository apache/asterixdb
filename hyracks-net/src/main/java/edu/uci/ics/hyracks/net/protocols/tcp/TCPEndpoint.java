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
package edu.uci.ics.hyracks.net.protocols.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TCPEndpoint {
    private final ITCPConnectionListener connectionListener;

    private ServerSocketChannel serverSocketChannel;

    private InetSocketAddress localAddress;

    private IOThread ioThread;

    public TCPEndpoint(ITCPConnectionListener connectionListener) {
        this.connectionListener = connectionListener;
    }

    public void start(InetSocketAddress localAddress) throws IOException {
        serverSocketChannel = ServerSocketChannel.open();
        ServerSocket serverSocket = serverSocketChannel.socket();
        serverSocket.bind(localAddress);
        this.localAddress = (InetSocketAddress) serverSocket.getLocalSocketAddress();
        ioThread = new IOThread();
        ioThread.registerServerSocket(serverSocketChannel);
        ioThread.start();
    }

    public void initiateConnection(InetSocketAddress remoteAddress) {
        ioThread.initiateConnection(remoteAddress);
    }

    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    private class IOThread extends Thread {
        private final List<InetSocketAddress>[] pendingConnections;

        private int writerIndex;

        private int readerIndex;

        private Selector selector;

        public IOThread() throws IOException {
            super("TCPEndpoint IO Thread");
            setPriority(MAX_PRIORITY);
            this.pendingConnections = new List[] { new ArrayList<InetSocketAddress>(),
                    new ArrayList<InetSocketAddress>() };
            writerIndex = 0;
            readerIndex = 1;
            selector = Selector.open();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    int n = selector.select();
                    swapReadersAndWriters();
                    if (!pendingConnections[readerIndex].isEmpty()) {
                        for (InetSocketAddress address : pendingConnections[readerIndex]) {
                            SocketChannel channel = SocketChannel.open();
                            channel.configureBlocking(false);
                            if (!channel.connect(address)) {
                                channel.register(selector, SelectionKey.OP_CONNECT);
                            } else {
                                SelectionKey key = channel.register(selector, 0);
                                createConnection(key, channel);
                            }
                        }
                        pendingConnections[readerIndex].clear();
                    }
                    if (n > 0) {
                        Iterator<SelectionKey> i = selector.selectedKeys().iterator();
                        while (i.hasNext()) {
                            SelectionKey key = i.next();
                            i.remove();
                            SelectableChannel sc = key.channel();
                            boolean readable = key.isReadable();
                            boolean writable = key.isWritable();

                            if (readable || writable) {
                                TCPConnection connection = (TCPConnection) key.attachment();
                                connection.getEventListener().notifyIOReady(connection, readable, writable);
                            }
                            if (key.isAcceptable()) {
                                assert sc == serverSocketChannel;
                                SocketChannel channel = serverSocketChannel.accept();
                                channel.configureBlocking(false);
                                SelectionKey sKey = channel.register(selector, 0);
                                TCPConnection connection = new TCPConnection(TCPEndpoint.this, channel, sKey, selector);
                                sKey.attach(connection);
                                connectionListener.acceptedConnection(connection);
                            } else if (key.isConnectable()) {
                                SocketChannel channel = (SocketChannel) sc;
                                if (channel.finishConnect()) {
                                    createConnection(key, channel);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private void createConnection(SelectionKey key, SocketChannel channel) {
            TCPConnection connection = new TCPConnection(TCPEndpoint.this, channel, key, selector);
            key.attach(connection);
            key.interestOps(0);
            connectionListener.connectionEstablished(connection);
        }

        synchronized void initiateConnection(InetSocketAddress remoteAddress) {
            pendingConnections[writerIndex].add(remoteAddress);
            selector.wakeup();
        }

        void registerServerSocket(ServerSocketChannel serverSocketChannel) throws IOException {
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        }

        private synchronized void swapReadersAndWriters() {
            int temp = readerIndex;
            readerIndex = writerIndex;
            writerIndex = temp;
        }
    }
}