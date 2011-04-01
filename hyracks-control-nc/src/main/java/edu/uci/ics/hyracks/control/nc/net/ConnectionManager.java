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
package edu.uci.ics.hyracks.control.nc.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.nc.partitions.IPartitionRequestListener;

public class ConnectionManager {
    private static final Logger LOGGER = Logger.getLogger(ConnectionManager.class.getName());

    private static final int INITIAL_MESSAGE_SIZE = 40;

    private final IHyracksRootContext ctx;

    private IPartitionRequestListener partitionRequestListener;

    private final ServerSocketChannel serverChannel;

    private volatile boolean stopped;

    private final ConnectionListenerThread connectionListener;

    private final DataListenerThread dataListener;

    private final NetworkAddress networkAddress;

    public ConnectionManager(IHyracksRootContext ctx, InetAddress inetAddress) throws IOException {
        this.ctx = ctx;
        serverChannel = ServerSocketChannel.open();
        ServerSocket serverSocket = serverChannel.socket();
        serverSocket.bind(new InetSocketAddress(inetAddress, 0));
        stopped = false;
        connectionListener = new ConnectionListenerThread();
        dataListener = new DataListenerThread();
        networkAddress = new NetworkAddress(serverSocket.getInetAddress(), serverSocket.getLocalPort());

    }

    public void setPartitionRequestListener(IPartitionRequestListener partitionRequestListener) {
        this.partitionRequestListener = partitionRequestListener;
    }

    public void start() {
        connectionListener.start();
        dataListener.start();
    }

    public void stop() {
        try {
            stopped = true;
            serverChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void connect(INetworkChannel channel) throws IOException {
        dataListener.addOutgoingConnection(channel);
    }

    private final class ConnectionListenerThread extends Thread {
        public ConnectionListenerThread() {
            super("Hyracks NC Connection Listener");
        }

        @Override
        public void run() {
            while (!stopped) {
                try {
                    SocketChannel sc = serverChannel.accept();
                    dataListener.addIncomingConnection(sc);
                } catch (AsynchronousCloseException e) {
                    // do nothing
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private final class DataListenerThread extends Thread {
        private Selector selector;

        private final List<SocketChannel> pendingIncomingConnections;
        private final List<SocketChannel> pendingNegotiations;
        private final List<INetworkChannel> pendingOutgoingConnections;
        private final List<INetworkChannel> pendingAbortConnections;

        public DataListenerThread() {
            super("Hyracks Data Listener Thread");
            try {
                selector = Selector.open();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            pendingIncomingConnections = new ArrayList<SocketChannel>();
            pendingNegotiations = new ArrayList<SocketChannel>();
            pendingOutgoingConnections = new ArrayList<INetworkChannel>();
            pendingAbortConnections = new ArrayList<INetworkChannel>();
        }

        synchronized void addIncomingConnection(SocketChannel sc) throws IOException {
            pendingIncomingConnections.add(sc);
            selector.wakeup();
        }

        synchronized void addOutgoingConnection(INetworkChannel channel) throws IOException {
            pendingOutgoingConnections.add(channel);
            selector.wakeup();
        }

        synchronized void addPendingAbortConnections(List<INetworkChannel> abortConnections) {
            pendingAbortConnections.addAll(abortConnections);
            selector.wakeup();
        }

        @Override
        public void run() {
            while (!stopped) {
                try {
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("Starting Select");
                    }
                    int n = selector.select();
                    synchronized (this) {
                        if (!pendingIncomingConnections.isEmpty()) {
                            for (SocketChannel sc : pendingIncomingConnections) {
                                sc.configureBlocking(false);
                                SelectionKey scKey = sc.register(selector, SelectionKey.OP_READ);
                                ByteBuffer buffer = ByteBuffer.allocate(INITIAL_MESSAGE_SIZE);
                                scKey.attach(buffer);
                                pendingNegotiations.add(sc);
                            }
                            pendingIncomingConnections.clear();
                        }
                        if (!pendingOutgoingConnections.isEmpty()) {
                            for (INetworkChannel nc : pendingOutgoingConnections) {
                                SocketAddress rAddr = nc.getRemoteAddress();
                                SocketChannel sc = SocketChannel.open();
                                sc.configureBlocking(false);
                                int interestOps = SelectionKey.OP_READ;
                                if (!sc.connect(rAddr)) {
                                    interestOps |= SelectionKey.OP_CONNECT;
                                }
                                SelectionKey scKey = sc.register(selector, interestOps);
                                scKey.attach(nc);
                                nc.setSelectionKey(scKey);
                            }
                            pendingOutgoingConnections.clear();
                        }
                        if (!pendingAbortConnections.isEmpty()) {
                            for (INetworkChannel nc : pendingAbortConnections) {
                                SelectionKey key = nc.getSelectionKey();
                                nc.abort();
                                nc.dispatchNetworkEvent();
                                key.cancel();
                            }
                            pendingAbortConnections.clear();
                        }
                        if (LOGGER.isLoggable(Level.FINE)) {
                            LOGGER.fine("Selector: " + n);
                        }
                        if (n > 0) {
                            for (Iterator<SelectionKey> i = selector.selectedKeys().iterator(); i.hasNext();) {
                                SelectionKey key = i.next();
                                i.remove();
                                SocketChannel sc = (SocketChannel) key.channel();
                                if (pendingNegotiations.contains(sc)) {
                                    if (key.isReadable()) {
                                        ByteBuffer buffer = (ByteBuffer) key.attachment();
                                        sc.read(buffer);
                                        buffer.flip();
                                        if (buffer.remaining() >= INITIAL_MESSAGE_SIZE) {
                                            PartitionId pid = readInitialMessage(buffer);
                                            key.interestOps(0);
                                            NetworkOutputChannel channel = new NetworkOutputChannel(ctx, 5);
                                            channel.setSelectionKey(key);
                                            key.attach(channel);
                                            try {
                                                partitionRequestListener.registerPartitionRequest(pid, channel);
                                            } catch (HyracksException e) {
                                                key.cancel();
                                                sc.close();
                                            }
                                        } else {
                                            buffer.compact();
                                        }
                                    }
                                } else {
                                    INetworkChannel channel = (INetworkChannel) key.attachment();
                                    boolean close = false;
                                    try {
                                        close = channel.dispatchNetworkEvent();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                        close = true;
                                    }
                                    if (close) {
                                        key.cancel();
                                        sc.close();
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private PartitionId readInitialMessage(ByteBuffer buffer) {
            UUID jobId = readUUID(buffer);
            ConnectorDescriptorId cdid = new ConnectorDescriptorId(readUUID(buffer));
            int senderIndex = buffer.getInt();
            int receiverIndex = buffer.getInt();
            return new PartitionId(jobId, cdid, senderIndex, receiverIndex);
        }

        private UUID readUUID(ByteBuffer buffer) {
            long msb = buffer.getLong();
            long lsb = buffer.getLong();
            return new UUID(msb, lsb);
        }
    }

    public NetworkAddress getNetworkAddress() {
        return networkAddress;
    }
}