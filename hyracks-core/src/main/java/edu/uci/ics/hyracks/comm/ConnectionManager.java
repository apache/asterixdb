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
package edu.uci.ics.hyracks.comm;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IConnectionEntry;
import edu.uci.ics.hyracks.api.comm.IDataReceiveListener;
import edu.uci.ics.hyracks.api.comm.IDataReceiveListenerFactory;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.comm.io.FrameConstants;
import edu.uci.ics.hyracks.comm.io.FrameHelper;

public class ConnectionManager {
    private static final Logger LOGGER = Logger.getLogger(ConnectionManager.class.getName());

    private static final int INITIAL_MESSAGE_LEN = 20;

    private NetworkAddress networkAddress;

    private ServerSocketChannel serverSocketChannel;

    private final IHyracksContext ctx;

    private final Map<UUID, IDataReceiveListenerFactory> pendingConnectionReceivers;

    private final ConnectionListenerThread connectionListenerThread;

    private final DataListenerThread dataListenerThread;

    private final IDataReceiveListener initialDataReceiveListener;

    private final Set<IConnectionEntry> connections;

    private volatile boolean stopped;

    private ByteBuffer emptyFrame;

    public ConnectionManager(IHyracksContext ctx, InetAddress address) throws IOException {
        this.ctx = ctx;
        serverSocketChannel = ServerSocketChannel.open();
        ServerSocket serverSocket = serverSocketChannel.socket();
        serverSocket.bind(new InetSocketAddress(address, 0));

        networkAddress = new NetworkAddress(serverSocket.getInetAddress(), serverSocket.getLocalPort());

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Connection manager listening on " + serverSocket.getInetAddress() + ":"
                + serverSocket.getLocalPort());
        }

        pendingConnectionReceivers = new HashMap<UUID, IDataReceiveListenerFactory>();
        dataListenerThread = new DataListenerThread();
        connectionListenerThread = new ConnectionListenerThread();
        initialDataReceiveListener = new InitialDataReceiveListener();
        emptyFrame = ctx.getResourceManager().allocateFrame();
        emptyFrame.putInt(FrameHelper.getTupleCountOffset(ctx), 0);
        connections = new HashSet<IConnectionEntry>();
    }

    public synchronized void dumpStats() {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Number of pendingConnectionReceivers: " + pendingConnectionReceivers.size());
            LOGGER.info("Number of selectable keys: " + dataListenerThread.selector.keys().size());
        }
    }

    public NetworkAddress getNetworkAddress() {
        return networkAddress;
    }

    public void start() {
        stopped = false;
        connectionListenerThread.start();
        dataListenerThread.start();
    }

    public void stop() {
        try {
            stopped = true;
            serverSocketChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public IFrameWriter connect(NetworkAddress address, UUID id, int senderId) throws HyracksDataException {
        try {
            SocketChannel channel = SocketChannel
                .open(new InetSocketAddress(address.getIpAddress(), address.getPort()));
            byte[] initialFrame = new byte[INITIAL_MESSAGE_LEN];
            ByteBuffer buffer = ByteBuffer.wrap(initialFrame);
            buffer.clear();
            buffer.putLong(id.getMostSignificantBits());
            buffer.putLong(id.getLeastSignificantBits());
            buffer.putInt(senderId);
            buffer.flip();
            int bytesWritten = 0;
            while (bytesWritten < INITIAL_MESSAGE_LEN) {
                int n = channel.write(buffer);
                if (n < 0) {
                    throw new HyracksDataException("Stream closed prematurely");
                }
                bytesWritten += n;
            }
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Send Initial message: " + id + ":" + senderId);
            }
            buffer.clear();
            buffer.limit(FrameConstants.SIZE_LEN);
            int bytesRead = 0;
            while (bytesRead < FrameConstants.SIZE_LEN) {
                int n = channel.read(buffer);
                if (n < 0) {
                    throw new HyracksDataException("Stream closed prematurely");
                }
                bytesRead += n;
            }
            buffer.flip();
            int frameLen = buffer.getInt();
            if (frameLen != FrameConstants.SIZE_LEN) {
                throw new IllegalStateException("Received illegal framelen = " + frameLen);
            }
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Got Ack message: " + id + ":" + senderId);
            }
            return new NetworkFrameWriter(channel);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public synchronized void acceptConnection(UUID id, IDataReceiveListenerFactory receiver) {
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.info("Connection manager accepting " + id);
        }
        pendingConnectionReceivers.put(id, receiver);
    }

    public synchronized void unacceptConnection(UUID id) {
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.info("Connection manager unaccepting " + id);
        }
        pendingConnectionReceivers.remove(id);
    }

    public synchronized void abortConnections(UUID jobId, UUID stageId) {
        List<IConnectionEntry> abortConnections = new ArrayList<IConnectionEntry>();
        synchronized (this) {
            for (IConnectionEntry ce : connections) {
                if (ce.getJobId().equals(jobId) && ce.getStageId().equals(stageId)) {
                    abortConnections.add(ce);
                }
            }
        }
        dataListenerThread.addPendingAbortConnections(abortConnections);
    }

    private final class NetworkFrameWriter implements IFrameWriter {
        private SocketChannel channel;

        NetworkFrameWriter(SocketChannel channel) {
            this.channel = channel;
        }

        @Override
        public void close() throws HyracksDataException {
            try {
                synchronized (emptyFrame) {
                    emptyFrame.position(0);
                    emptyFrame.limit(emptyFrame.capacity());
                    channel.write(emptyFrame);
                }
                channel.close();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            try {
                if (LOGGER.isLoggable(Level.FINER)) {
                    int frameLen = buffer.getInt(buffer.position());
                    LOGGER.finer("ConnectionManager.NetworkFrameWriter: frameLen = " + frameLen);
                }
                while (buffer.remaining() > 0) {
                    channel.write(buffer);
                }
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void open() throws HyracksDataException {
        }
    }

    private final class ConnectionListenerThread extends Thread {
        public ConnectionListenerThread() {
            super("Hyracks Connection Listener Thread");
        }

        @Override
        public void run() {
            while (!stopped) {
                try {
                    SocketChannel sc = serverSocketChannel.accept();
                    dataListenerThread.addSocketChannel(sc);
                } catch (AsynchronousCloseException e) {
                    // do nothing
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private final class DataListenerThread extends Thread {
        private Selector selector;

        private List<SocketChannel> pendingNewSockets;
        private List<IConnectionEntry> pendingAbortConnections;

        public DataListenerThread() {
            super("Hyracks Data Listener Thread");
            try {
                selector = Selector.open();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            pendingNewSockets = new ArrayList<SocketChannel>();
            pendingAbortConnections = new ArrayList<IConnectionEntry>();
        }

        synchronized void addSocketChannel(SocketChannel sc) throws IOException {
            pendingNewSockets.add(sc);
            selector.wakeup();
        }

        synchronized void addPendingAbortConnections(List<IConnectionEntry> abortConnections) {
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
                        if (!pendingNewSockets.isEmpty()) {
                            for (SocketChannel sc : pendingNewSockets) {
                                sc.configureBlocking(false);
                                SelectionKey scKey = sc.register(selector, SelectionKey.OP_READ);
                                ConnectionEntry entry = new ConnectionEntry(ctx, sc, scKey);
                                entry.setDataReceiveListener(initialDataReceiveListener);
                                scKey.attach(entry);
                                if (LOGGER.isLoggable(Level.FINE)) {
                                    LOGGER.fine("Woke up selector");
                                }
                            }
                            pendingNewSockets.clear();
                        }
                        if (!pendingAbortConnections.isEmpty()) {
                            for (IConnectionEntry ce : pendingAbortConnections) {
                                SelectionKey key = ce.getSelectionKey();
                                ce.abort();
                                ((ConnectionEntry) ce).dispatch(key);
                                key.cancel();
                                ce.close();
                                synchronized (ConnectionManager.this) {
                                    connections.remove(ce);
                                }
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
                                ConnectionEntry entry = (ConnectionEntry) key.attachment();
                                boolean close = false;
                                try {
                                    close = entry.dispatch(key);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                    close = true;
                                }
                                if (close) {
                                    key.cancel();
                                    entry.close();
                                    synchronized (ConnectionManager.this) {
                                        connections.remove(entry);
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
    }

    private class InitialDataReceiveListener implements IDataReceiveListener {
        @Override
        public void dataReceived(IConnectionEntry entry) throws IOException {
            ByteBuffer buffer = entry.getReadBuffer();
            buffer.flip();
            IDataReceiveListener newListener = null;
            if (buffer.remaining() >= INITIAL_MESSAGE_LEN) {
                long msb = buffer.getLong();
                long lsb = buffer.getLong();
                UUID endpointID = new UUID(msb, lsb);
                int senderId = buffer.getInt();
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine("Initial Frame received: " + endpointID + ":" + senderId);
                }
                IDataReceiveListenerFactory connectionReceiver;
                synchronized (ConnectionManager.this) {
                    connectionReceiver = pendingConnectionReceivers.get(endpointID);
                    if (connectionReceiver == null) {
                        entry.close();
                        return;
                    }
                }

                newListener = connectionReceiver.getDataReceiveListener(endpointID, entry, senderId);
                entry.setDataReceiveListener(newListener);
                entry.setJobId(connectionReceiver.getJobId());
                entry.setStageId(connectionReceiver.getStageId());
                synchronized (ConnectionManager.this) {
                    connections.add(entry);
                }
                byte[] ack = new byte[4];
                ByteBuffer ackBuffer = ByteBuffer.wrap(ack);
                ackBuffer.clear();
                ackBuffer.putInt(FrameConstants.SIZE_LEN);
                ackBuffer.flip();
                entry.write(ackBuffer);
            }
            buffer.compact();
            if (newListener != null && buffer.remaining() > 0) {
                newListener.dataReceived(entry);
            }
        }

        @Override
        public void eos(IConnectionEntry entry) {
        }
    }
}