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
import java.net.ServerSocket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;

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
        this.serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().setReuseAddress(true);
        serverSocketChannel.configureBlocking(false);
        ServerSocket socket = serverSocketChannel.socket();
        socket.bind(socketAddress);
        address = new InetSocketAddress(socket.getInetAddress(), socket.getLocalPort());
        networkThread = new NetworkThread();
        networkThread.setPriority(Thread.MAX_PRIORITY);
        ipcHandleMap = new HashMap<>();
        pendingConnections = new ArrayList<>();
        workingPendingConnections = new ArrayList<>();
        sendList = new ArrayList<>();
        workingSendList = new ArrayList<>();
    }

    InetSocketAddress getAddress() {
        return address;
    }

    void start() {
        stopped = false;
        networkThread.start();
    }

    void stop() {
        stopped = true;
        IOUtils.closeQuietly(serverSocketChannel);
        networkThread.selector.wakeup();
    }

    IPCHandle getIPCHandle(InetSocketAddress remoteAddress, int retries) throws IOException, InterruptedException {
        IPCHandle handle;
        int attempt = 1;
        while (true) {
            synchronized (this) {
                handle = ipcHandleMap.get(remoteAddress);
                if (handle == null || !handle.isConnected()) {
                    handle = new IPCHandle(system, remoteAddress);
                    pendingConnections.add(handle);
                    networkThread.selector.wakeup();
                }
            }
            if (handle.waitTillConnected()) {
                return handle;
            }
            if (retries < 0) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Connection to " + remoteAddress + " failed, retrying...");
                    attempt++;
                    Thread.sleep(5000);
                }
            } else if (attempt < retries) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Connection to " + remoteAddress + " failed (Attempt " + attempt + " of " + retries
                            + ")");
                    attempt++;
                    Thread.sleep(5000);
                }
            } else {
                throw new IOException("Connection failed to " + remoteAddress);
            }
        }

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

        private final Set<SocketChannel> openChannels = new HashSet<>();

        public NetworkThread() {
            super("IPC Network Listener Thread [" + address + "]");
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
                doRun();
            } finally {
                cleanup();
            }
        }

        private void doRun() {
            try {
                serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            } catch (ClosedChannelException e) {
                throw new RuntimeException(e);
            }
            BitSet unsentMessagesBitmap = new BitSet();
            List<Message> tempUnsentMessages = new ArrayList<>();
            int failingLoops = 0;
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
                            openChannels.add(channel);
                            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                            channel.configureBlocking(false);
                            SelectionKey cKey;
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
                                    IOUtils.closeQuietly(channel);
                                    openChannels.remove(channel);
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
                                    IOUtils.closeQuietly(channel);
                                    openChannels.remove(channel);
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
                                openChannels.add(channel);
                                channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                                channel.configureBlocking(false);
                                IPCHandle handle = new IPCHandle(system, null);
                                SelectionKey cKey = channel.register(selector, SelectionKey.OP_READ);
                                handle.setKey(cKey);
                                cKey.attach(handle);
                                handle.setState(HandleState.CONNECT_RECEIVED);
                            } else if (key.isConnectable()) {
                                SocketChannel channel = (SocketChannel) sc;
                                IPCHandle handle = (IPCHandle) key.attachment();
                                if (!finishConnect(channel)) {
                                    handle.setState(HandleState.CONNECT_FAILED);
                                    continue;
                                }

                                handle.setState(HandleState.CONNECT_SENT);
                                registerHandle(handle);
                                key.interestOps(SelectionKey.OP_READ);
                                write(createInitialReqMessage(handle));
                            }
                        }
                    }
                    // reset failingLoops on a good loop
                    failingLoops = 0;
                } catch (Exception e) {
                    int sleepSecs = (int)Math.pow(2, Math.min(11, failingLoops++));
                    LOGGER.log(Level.SEVERE, "Exception processing message; sleeping " + sleepSecs
                            + " seconds", e);
                    try {
                        Thread.sleep(TimeUnit.SECONDS.toMillis(sleepSecs));
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        private void cleanup() {
            for (Channel channel : openChannels) {
                IOUtils.closeQuietly(channel);
            }
            openChannels.clear();
            IOUtils.closeQuietly(selector);
        }

        private boolean finishConnect(SocketChannel channel) {
            boolean connectFinished = false;
            try {
                connectFinished = channel.finishConnect();
                if (!connectFinished) {
                    LOGGER.log(Level.WARNING, "Channel connect did not finish");
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Exception finishing channel connect", e);
            }
            return connectFinished;
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
