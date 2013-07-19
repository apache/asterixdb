/*
 * Copyright 2009-2013 by The Regents of the University of California
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
import java.net.ServerSocket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class IPCConnectionManager {
    private static final Logger LOGGER = Logger.getLogger(IPCConnectionManager.class.getName());

    private final IPCSystem system;

    private final NetworkThread networkThread;

    private final ServerSocketChannel serverSocketChannel;

    private final Map<InetSocketAddress, IPCHandle> ipcHandleMap;

    private final List<IPCHandle> pendingConnections;

    private final List<IPCHandle> workingPendingConnections;

    private final List<Message> sendList;

    private final List<Message> workingSendList;

    private final InetSocketAddress address;

    private volatile boolean stopped;

    IPCConnectionManager(IPCSystem system, InetSocketAddress socketAddress) throws IOException {
        this.system = system;
        this.networkThread = new NetworkThread();
        this.serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().setReuseAddress(true);
        serverSocketChannel.configureBlocking(false);
        ServerSocket socket = serverSocketChannel.socket();
        socket.bind(socketAddress);
        address = new InetSocketAddress(socket.getInetAddress(), socket.getLocalPort());
        ipcHandleMap = new HashMap<InetSocketAddress, IPCHandle>();
        pendingConnections = new ArrayList<IPCHandle>();
        workingPendingConnections = new ArrayList<IPCHandle>();
        sendList = new ArrayList<Message>();
        workingSendList = new ArrayList<Message>();
    }

    InetSocketAddress getAddress() {
        return address;
    }

    void start() {
        stopped = false;
        networkThread.start();
    }

    void stop() throws IOException {
        stopped = true;
        serverSocketChannel.close();
    }

    IPCHandle getIPCHandle(InetSocketAddress remoteAddress) throws IOException, InterruptedException {
        IPCHandle handle;
        synchronized (this) {
            handle = ipcHandleMap.get(remoteAddress);
            if (handle == null) {
                handle = new IPCHandle(system, remoteAddress);
                pendingConnections.add(handle);
                networkThread.selector.wakeup();
            }
        }
        handle.waitTillConnected();
        return handle;
    }

    synchronized void registerHandle(IPCHandle handle) {
        ipcHandleMap.put(handle.getRemoteAddress(), handle);
    }

    synchronized void write(Message msg) {
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Enqueued message: " + msg);
        }
        sendList.add(msg);
        networkThread.selector.wakeup();
    }

    private synchronized void collectOutstandingWork() {
        if (!pendingConnections.isEmpty()) {
            moveAll(pendingConnections, workingPendingConnections);
        }
        if (!sendList.isEmpty()) {
            moveAll(sendList, workingSendList);
        }
    }

    private Message createInitialReqMessage(IPCHandle handle) {
        Message msg = new Message(handle);
        msg.setMessageId(system.createMessageId());
        msg.setRequestMessageId(-1);
        msg.setFlag(Message.INITIAL_REQ);
        msg.setPayload(address);
        return msg;
    }

    private Message createInitialAckMessage(IPCHandle handle, Message req) {
        Message msg = new Message(handle);
        msg.setMessageId(system.createMessageId());
        msg.setRequestMessageId(req.getMessageId());
        msg.setFlag(Message.INITIAL_ACK);
        msg.setPayload(null);
        return msg;
    }

    void ack(IPCHandle handle, Message req) {
        write(createInitialAckMessage(handle, req));
    }

    private class NetworkThread extends Thread {
        private final Selector selector;

        public NetworkThread() {
            super("IPC Network Listener Thread");
            setDaemon(true);
            try {
                selector = Selector.open();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void run() {
            try {
                serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            } catch (ClosedChannelException e) {
                throw new RuntimeException(e);
            }
            BitSet unsentMessagesBitmap = new BitSet();
            List<Message> tempUnsentMessages = new ArrayList<Message>();
            while (!stopped) {
                try {
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("Starting Select");
                    }
                    int n = selector.select();
                    collectOutstandingWork();
                    if (!workingPendingConnections.isEmpty()) {
                        for (IPCHandle handle : workingPendingConnections) {
                            SocketChannel channel = SocketChannel.open();
                            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                            channel.configureBlocking(false);
                            SelectionKey cKey = null;
                            if (channel.connect(handle.getRemoteAddress())) {
                                cKey = channel.register(selector, SelectionKey.OP_READ);
                                handle.setState(HandleState.CONNECT_SENT);
                                write(createInitialReqMessage(handle));
                            } else {
                                cKey = channel.register(selector, SelectionKey.OP_CONNECT);
                            }
                            handle.setKey(cKey);
                            cKey.attach(handle);
                        }
                        workingPendingConnections.clear();
                    }
                    if (!workingSendList.isEmpty()) {
                        unsentMessagesBitmap.clear();
                        int len = workingSendList.size();
                        for (int i = 0; i < len; ++i) {
                            Message msg = workingSendList.get(i);
                            if (LOGGER.isLoggable(Level.FINE)) {
                                LOGGER.fine("Processing send of message: " + msg);
                            }
                            IPCHandle handle = msg.getIPCHandle();
                            if (handle.getState() != HandleState.CLOSED) {
                                if (!handle.full()) {
                                    while (true) {
                                        ByteBuffer buffer = handle.getOutBuffer();
                                        buffer.compact();
                                        boolean success = msg.write(buffer);
                                        buffer.flip();
                                        if (success) {
                                            system.getPerformanceCounters().addMessageSentCount(1);
                                            SelectionKey key = handle.getKey();
                                            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                                        } else {
                                            if (!buffer.hasRemaining()) {
                                                handle.resizeOutBuffer();
                                                continue;
                                            }
                                            handle.markFull();
                                            unsentMessagesBitmap.set(i);
                                        }
                                        break;
                                    }
                                } else {
                                    unsentMessagesBitmap.set(i);
                                }
                            }
                        }
                        copyUnsentMessages(unsentMessagesBitmap, tempUnsentMessages);
                    }
                    if (n > 0) {
                        for (Iterator<SelectionKey> i = selector.selectedKeys().iterator(); i.hasNext();) {
                            SelectionKey key = i.next();
                            i.remove();
                            SelectableChannel sc = key.channel();
                            if (key.isReadable()) {
                                SocketChannel channel = (SocketChannel) sc;
                                IPCHandle handle = (IPCHandle) key.attachment();
                                ByteBuffer readBuffer = handle.getInBuffer();
                                int len = channel.read(readBuffer);
                                system.getPerformanceCounters().addMessageBytesReceived(len);
                                if (len < 0) {
                                    key.cancel();
                                    channel.close();
                                    handle.close();
                                } else {
                                    handle.processIncomingMessages();
                                    if (!readBuffer.hasRemaining()) {
                                        handle.resizeInBuffer();
                                    }
                                }
                            } else if (key.isWritable()) {
                                SocketChannel channel = (SocketChannel) sc;
                                IPCHandle handle = (IPCHandle) key.attachment();
                                ByteBuffer writeBuffer = handle.getOutBuffer();
                                int len = channel.write(writeBuffer);
                                system.getPerformanceCounters().addMessageBytesSent(len);
                                if (len < 0) {
                                    key.cancel();
                                    channel.close();
                                    handle.close();
                                } else if (!writeBuffer.hasRemaining()) {
                                    key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                                }
                                if (handle.full()) {
                                    handle.clearFull();
                                    selector.wakeup();
                                }
                            } else if (key.isAcceptable()) {
                                assert sc == serverSocketChannel;
                                SocketChannel channel = serverSocketChannel.accept();
                                channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                                channel.configureBlocking(false);
                                IPCHandle handle = new IPCHandle(system, null);
                                SelectionKey cKey = channel.register(selector, SelectionKey.OP_READ);
                                handle.setKey(cKey);
                                cKey.attach(handle);
                                handle.setState(HandleState.CONNECT_RECEIVED);
                            } else if (key.isConnectable()) {
                                SocketChannel channel = (SocketChannel) sc;
                                if (channel.finishConnect()) {
                                    IPCHandle handle = (IPCHandle) key.attachment();
                                    handle.setState(HandleState.CONNECT_SENT);
                                    registerHandle(handle);
                                    key.interestOps(SelectionKey.OP_READ);
                                    write(createInitialReqMessage(handle));
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private void copyUnsentMessages(BitSet unsentMessagesBitmap, List<Message> tempUnsentMessages) {
            assert tempUnsentMessages.isEmpty();
            for (int i = unsentMessagesBitmap.nextSetBit(0); i >= 0; i = unsentMessagesBitmap.nextSetBit(i + 1)) {
                tempUnsentMessages.add(workingSendList.get(i));
            }
            workingSendList.clear();
            moveAll(tempUnsentMessages, workingSendList);
        }
    }

    private <T> void moveAll(List<T> source, List<T> target) {
        int len = source.size();
        for (int i = 0; i < len; ++i) {
            target.add(source.get(i));
        }
        source.clear();
    }
}