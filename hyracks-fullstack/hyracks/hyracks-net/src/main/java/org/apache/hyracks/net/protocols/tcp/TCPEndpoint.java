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

import static org.apache.hyracks.net.protocols.tcp.TCPConnection.ConnectionType;
import static org.apache.hyracks.net.protocols.tcp.TCPConnection.ConnectionType.INCOMING;
import static org.apache.hyracks.net.protocols.tcp.TCPConnection.ConnectionType.OUTGOING;

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
import java.util.concurrent.CompletableFuture;

import org.apache.hyracks.api.network.ISocketChannel;
import org.apache.hyracks.api.network.ISocketChannelFactory;
import org.apache.hyracks.util.NetworkUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TCPEndpoint {

    private static final Logger LOGGER = LogManager.getLogger();

    private final ITCPConnectionListener connectionListener;

    private final int nThreads;

    private ServerSocketChannel serverSocketChannel;

    private InetSocketAddress localAddress;

    private IOThread[] ioThreads;

    private int nextThread;

    private final ISocketChannelFactory socketChannelFactory;

    public TCPEndpoint(ITCPConnectionListener connectionListener, int nThreads,
            ISocketChannelFactory socketChannelFactory) {
        this.connectionListener = connectionListener;
        this.nThreads = nThreads;
        this.socketChannelFactory = socketChannelFactory;
    }

    public void start(InetSocketAddress localAddress) throws IOException {
        // Setup a server socket listening channel only if the TCPEndpoint is a listening endpoint.
        if (localAddress != null) {
            serverSocketChannel = ServerSocketChannel.open();
            ServerSocket serverSocket = serverSocketChannel.socket();
            serverSocket.bind(localAddress);
            this.localAddress = (InetSocketAddress) serverSocket.getLocalSocketAddress();
        }
        ioThreads = new IOThread[nThreads];
        for (int i = 0; i < ioThreads.length; ++i) {
            ioThreads[i] = new IOThread();
        }
        if (localAddress != null) {
            for (IOThread ioThread : ioThreads) {
                ioThread.registerServerSocket(serverSocketChannel);
            }
        }
        for (IOThread ioThread : ioThreads) {
            ioThread.start();
        }
    }

    private synchronized int getNextThread() {
        int result = nextThread;
        nextThread = (nextThread + 1) % nThreads;
        return result;
    }

    public void initiateConnection(InetSocketAddress remoteAddress) {
        int targetThread = getNextThread();
        ioThreads[targetThread].initiateConnection(remoteAddress);
    }

    private void distributeIncomingConnection(SocketChannel channel) {
        int targetThread = getNextThread();
        ioThreads[targetThread].addIncomingConnection(channel);
    }

    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    public String toString() {
        return "TCPEndpoint [Local Address: " + localAddress + "]";
    }

    private class IOThread extends Thread {
        private final List<InetSocketAddress> pendingConnections;

        private final List<InetSocketAddress> workingPendingConnections;

        private final List<SocketChannel> incomingConnections;

        private final List<SocketChannel> workingIncomingConnections;

        private final List<PendingHandshakeConnection> handshakeCompletedConnections;

        private final Selector selector;

        public IOThread() throws IOException {
            super("TCPEndpoint IO Thread [" + localAddress + "]");
            setDaemon(true);
            setPriority(Thread.NORM_PRIORITY);
            this.pendingConnections = new ArrayList<>();
            this.workingPendingConnections = new ArrayList<>();
            this.incomingConnections = new ArrayList<>();
            this.workingIncomingConnections = new ArrayList<>();
            handshakeCompletedConnections = new ArrayList<>();
            selector = Selector.open();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    int n = selector.select();
                    collectOutstandingWork();
                    if (!workingPendingConnections.isEmpty()) {
                        for (InetSocketAddress address : workingPendingConnections) {
                            SocketChannel channel = SocketChannel.open();
                            register(channel);
                            boolean connect = false;
                            boolean failure = false;
                            try {
                                connect = channel.connect(address);
                            } catch (IOException e) {
                                failure = true;
                                synchronized (connectionListener) {
                                    connectionListener.connectionFailure(address, e);
                                }
                            }
                            if (!failure) {
                                if (!connect) {
                                    SelectionKey key = channel.register(selector, SelectionKey.OP_CONNECT);
                                    key.attach(address);
                                } else {
                                    socketConnected(address, channel);
                                }
                            }
                        }
                        workingPendingConnections.clear();
                    }
                    if (!workingIncomingConnections.isEmpty()) {
                        for (SocketChannel channel : workingIncomingConnections) {
                            register(channel);
                            connectionReceived(channel);
                        }
                        workingIncomingConnections.clear();
                    }
                    synchronized (handshakeCompletedConnections) {
                        if (!handshakeCompletedConnections.isEmpty()) {
                            for (final PendingHandshakeConnection conn : handshakeCompletedConnections) {
                                handshakeCompleted(conn);
                            }
                            handshakeCompletedConnections.clear();
                        }
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
                                try {
                                    connection.getEventListener().notifyIOReady(connection, readable, writable);
                                } catch (Exception e) {
                                    LOGGER.error("Unexpected tcp io error in connection {}", connection, e);
                                    connection.getEventListener().notifyIOError(e);
                                    connection.close();
                                    synchronized (connectionListener) {
                                        connectionListener.connectionClosed(connection);
                                    }
                                    continue;
                                }
                            }
                            if (key.isAcceptable()) {
                                assert sc == serverSocketChannel;
                                SocketChannel channel = serverSocketChannel.accept();
                                distributeIncomingConnection(channel);
                            } else if (key.isConnectable()) {
                                SocketChannel channel = (SocketChannel) sc;
                                boolean finishConnect = false;
                                try {
                                    finishConnect = channel.finishConnect();
                                } catch (IOException e) {
                                    LOGGER.error("Failed to finish connect to channel {}", key.attachment(), e);
                                    key.cancel();
                                    synchronized (connectionListener) {
                                        connectionListener.connectionFailure((InetSocketAddress) key.attachment(), e);
                                    }
                                }
                                if (finishConnect) {
                                    socketConnected((InetSocketAddress) key.attachment(), channel);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("Error in TCPEndpoint {}", TCPEndpoint.this, e);
                }
            }
        }

        private void handshakeCompleted(PendingHandshakeConnection conn) {
            try {
                if (conn.handshakeSuccess) {
                    final SelectionKey key = conn.socketChannel.getSocketChannel().register(selector, 0);
                    final TCPConnection tcpConn =
                            new TCPConnection(TCPEndpoint.this, conn.socketChannel, key, selector, conn.type);
                    key.attach(tcpConn);
                    switch (conn.type) {
                        case INCOMING:
                            connectionAccepted(tcpConn);
                            break;
                        case OUTGOING:
                            connectionEstablished(tcpConn);
                            break;
                        default:
                            throw new IllegalStateException("Unknown connection type: " + conn.type);
                    }
                } else {
                    handleHandshakeFailure(conn);
                }
            } catch (Exception e) {
                LOGGER.error("failed to establish connection after handshake", e);
                handleHandshakeFailure(conn);
            }
        }

        synchronized void initiateConnection(InetSocketAddress remoteAddress) {
            pendingConnections.add(remoteAddress);
            selector.wakeup();
        }

        synchronized void addIncomingConnection(SocketChannel channel) {
            incomingConnections.add(channel);
            selector.wakeup();
        }

        void registerServerSocket(ServerSocketChannel serverSocketChannel) throws IOException {
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        }

        private synchronized void collectOutstandingWork() {
            if (!pendingConnections.isEmpty()) {
                workingPendingConnections.addAll(pendingConnections);
                pendingConnections.clear();
            }
            if (!incomingConnections.isEmpty()) {
                workingIncomingConnections.addAll(incomingConnections);
                incomingConnections.clear();
            }
        }

        private void register(SocketChannel channel) throws IOException {
            NetworkUtil.configure(channel);
            channel.configureBlocking(false);
        }

        private void socketConnected(InetSocketAddress remoteAddress, SocketChannel channel) {
            final ISocketChannel socketChannel = socketChannelFactory.createClientChannel(channel);
            final PendingHandshakeConnection conn =
                    new PendingHandshakeConnection(socketChannel, remoteAddress, OUTGOING);
            if (socketChannel.requiresHandshake()) {
                asyncHandshake(conn);
            } else {
                handshakeCompleted(true, conn);
            }
        }

        private void connectionReceived(SocketChannel channel) {
            final ISocketChannel socketChannel = socketChannelFactory.createServerChannel(channel);
            final PendingHandshakeConnection conn = new PendingHandshakeConnection(socketChannel, null, INCOMING);
            if (socketChannel.requiresHandshake()) {
                asyncHandshake(conn);
            } else {
                handshakeCompleted(true, conn);
            }
        }

        private void asyncHandshake(PendingHandshakeConnection connection) {
            CompletableFuture.supplyAsync(connection.socketChannel::handshake).exceptionally(ex -> false)
                    .thenAccept(handshakeSuccess -> handleHandshakeCompletion(handshakeSuccess, connection));
        }

        private void handleHandshakeCompletion(Boolean handshakeSuccess, PendingHandshakeConnection conn) {
            handshakeCompleted(handshakeSuccess, conn);
            selector.wakeup();
        }

        private void handshakeCompleted(Boolean handshakeSuccess, PendingHandshakeConnection conn) {
            conn.handshakeSuccess = handshakeSuccess;
            synchronized (handshakeCompletedConnections) {
                handshakeCompletedConnections.add(conn);
            }
        }

        private void connectionEstablished(TCPConnection connection) {
            synchronized (connectionListener) {
                connectionListener.connectionEstablished(connection);
            }
        }

        private void connectionAccepted(TCPConnection connection) {
            synchronized (connectionListener) {
                connectionListener.acceptedConnection(connection);
            }
        }

        private void handleHandshakeFailure(PendingHandshakeConnection conn) {
            NetworkUtil.closeQuietly(conn.socketChannel);
            if (conn.type == OUTGOING) {
                synchronized (connectionListener) {
                    connectionListener.connectionFailure(conn.address, new IOException("handshake failure"));
                }
            }
        }
    }

    private static class PendingHandshakeConnection {

        private final ISocketChannel socketChannel;
        private final ConnectionType type;
        private final InetSocketAddress address;
        private boolean handshakeSuccess = false;

        PendingHandshakeConnection(ISocketChannel socketChannel, InetSocketAddress address,
                ConnectionType connectionType) {
            this.socketChannel = socketChannel;
            this.type = connectionType;
            this.address = address;
        }
    }
}
