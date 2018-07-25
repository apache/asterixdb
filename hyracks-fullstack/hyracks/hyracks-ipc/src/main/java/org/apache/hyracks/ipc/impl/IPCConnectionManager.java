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
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
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

import org.apache.hyracks.util.ExitUtil;
import org.apache.hyracks.util.NetworkUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IPCConnectionManager {
    private static final Logger LOGGER = LogManager.getLogger();

    // TODO(mblow): the next two could be config parameters
    private static final int INITIAL_RETRY_DELAY_MILLIS = 100;
    private static final int MAX_RETRY_DELAY_MILLIS = 15000;

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
        NetworkUtil.closeQuietly(serverSocketChannel);
        networkThread.selector.wakeup();
    }

    IPCHandle getIPCHandle(InetSocketAddress remoteAddress, int maxRetries) throws IOException, InterruptedException {
        IPCHandle handle;
        int retries = 0;
        int delay = INITIAL_RETRY_DELAY_MILLIS;
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
            if (maxRetries < 0 || retries++ < maxRetries) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Connection to " + remoteAddress + " failed; retrying" + (maxRetries <= 0 ? ""
                            : " (retry attempt " + retries + " of " + maxRetries + ") after " + delay + "ms"));
                }
                Thread.sleep(delay);
                delay = Math.min(MAX_RETRY_DELAY_MILLIS, (int) (delay * 1.5));
            } else {
                throw new IOException("Connection failed to " + remoteAddress);
            }
        }

    }

    synchronized void registerHandle(IPCHandle handle) {
        ipcHandleMap.put(handle.getRemoteAddress(), handle);
    }

    synchronized void write(Message msg) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Enqueued message: " + msg);
        }
        sendList.add(msg);
        networkThread.selector.wakeup();
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
        private final BitSet unsentMessagesBitmap = new BitSet();
        private final List<Message> tempUnsentMessages = new ArrayList<>();

        NetworkThread() {
            super("IPC Network Listener Thread [" + address + "]");
            setDaemon(true);
            try {
                selector = Selector.open();
                serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            } catch (IOException e) {
                throw new IllegalStateException(e);
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
            int failingLoops = 0;
            while (!stopped) {
                try {
                    int n = selector.select();
                    collectOutstandingWork();
                    if (!workingPendingConnections.isEmpty()) {
                        establishPendingConnections();
                    }
                    if (!workingSendList.isEmpty()) {
                        sendPendingMessages();
                    }
                    if (n > 0) {
                        processSelectedKeys();
                    }
                    // reset failingLoops on a good loop
                    failingLoops = 0;
                } catch (Exception e) {
                    int sleepSecs = (int) Math.pow(2, Math.min(11, failingLoops++));
                    LOGGER.log(Level.ERROR, "Exception processing message; sleeping " + sleepSecs + " seconds", e);
                    try {
                        Thread.sleep(TimeUnit.SECONDS.toMillis(sleepSecs));
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        private void processSelectedKeys() {
            for (Iterator<SelectionKey> i = selector.selectedKeys().iterator(); i.hasNext();) {
                SelectionKey key = i.next();
                i.remove();
                final SelectableChannel sc = key.channel();
                if (key.isReadable()) {
                    read(key);
                } else if (key.isWritable()) {
                    write(key);
                } else if (key.isAcceptable()) {
                    assert sc == serverSocketChannel;
                    accept();
                } else if (key.isConnectable()) {
                    finishConnect(key);
                }
            }
        }

        private void finishConnect(SelectionKey connectableKey) {
            SocketChannel channel = (SocketChannel) connectableKey.channel();
            IPCHandle handle = (IPCHandle) connectableKey.attachment();
            boolean connected = false;
            try {
                connected = channel.finishConnect();
                if (connected) {
                    connectableKey.interestOps(SelectionKey.OP_READ);
                    connectionEstablished(handle);
                }
            } catch (IOException e) {
                LOGGER.warn("Exception finishing connect", e);
            } finally {
                if (!connected) {
                    LOGGER.warn("Failed to finish connect to {}", handle.getRemoteAddress());
                    close(connectableKey, channel);
                }
            }
        }

        private void accept() {
            SocketChannel channel = null;
            SelectionKey channelKey = null;
            try {
                channel = serverSocketChannel.accept();
                register(channel);
                channelKey = channel.register(selector, SelectionKey.OP_READ);
                IPCHandle handle = new IPCHandle(system, null);
                handle.setKey(channelKey);
                channelKey.attach(handle);
                handle.setState(HandleState.CONNECT_RECEIVED);
            } catch (IOException e) {
                LOGGER.error("Failed to accept channel ", e);
                close(channelKey, channel);
            }
        }

        private void establishPendingConnections() {
            for (IPCHandle handle : workingPendingConnections) {
                SocketChannel channel = null;
                SelectionKey channelKey = null;
                try {
                    channel = SocketChannel.open();
                    register(channel);
                    if (channel.connect(handle.getRemoteAddress())) {
                        channelKey = channel.register(selector, SelectionKey.OP_READ);
                        connectionEstablished(handle);
                    } else {
                        channelKey = channel.register(selector, SelectionKey.OP_CONNECT);
                    }
                    handle.setKey(channelKey);
                    channelKey.attach(handle);
                } catch (IOException e) {
                    LOGGER.error("Failed to accept channel ", e);
                    close(channelKey, channel);
                }
            }
            workingPendingConnections.clear();
        }

        private void connectionEstablished(IPCHandle handle) {
            handle.setState(HandleState.CONNECT_SENT);
            registerHandle(handle);
            IPCConnectionManager.this.write(createInitialReqMessage(handle));
        }

        private void sendPendingMessages() {
            unsentMessagesBitmap.clear();
            int len = workingSendList.size();
            for (int i = 0; i < len; ++i) {
                Message msg = workingSendList.get(i);
                final boolean sent = sendMessage(msg);
                if (!sent) {
                    unsentMessagesBitmap.set(i);
                }
            }
            copyUnsentMessages(unsentMessagesBitmap, tempUnsentMessages);
        }

        private boolean sendMessage(Message msg) {
            LOGGER.debug("Processing send of message: {}", msg);
            IPCHandle handle = msg.getIPCHandle();
            if (handle.getState() == HandleState.CLOSED) {
                // message will never be sent
                return true;
            }
            if (handle.full()) {
                return false;
            }
            try {
                while (true) {
                    ByteBuffer buffer = handle.getOutBuffer();
                    buffer.compact();
                    boolean success = msg.write(buffer);
                    buffer.flip();
                    if (success) {
                        system.getPerformanceCounters().addMessageSentCount(1);
                        SelectionKey key = handle.getKey();
                        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                        return true;
                    } else {
                        if (!buffer.hasRemaining()) {
                            handle.resizeOutBuffer();
                            continue;
                        }
                        handle.markFull();
                        return false;
                    }
                }
            } catch (Exception e) {
                LOGGER.fatal("Unrecoverable networking failure; Halting...", e);
                ExitUtil.halt(ExitUtil.EC_NETWORK_FAILURE);
            }
            return false;
        }

        private void cleanup() {
            for (Channel channel : openChannels) {
                NetworkUtil.closeQuietly(channel);
            }
            openChannels.clear();
            NetworkUtil.closeQuietly(selector);
        }

        private void copyUnsentMessages(BitSet unsentMessagesBitmap, List<Message> tempUnsentMessages) {
            assert tempUnsentMessages.isEmpty();
            for (int i = unsentMessagesBitmap.nextSetBit(0); i >= 0; i = unsentMessagesBitmap.nextSetBit(i + 1)) {
                tempUnsentMessages.add(workingSendList.get(i));
            }
            workingSendList.clear();
            moveAll(tempUnsentMessages, workingSendList);
        }

        private void register(SocketChannel channel) throws IOException {
            openChannels.add(channel);
            NetworkUtil.configure(channel);
            channel.configureBlocking(false);
        }

        private void read(SelectionKey readableKey) {
            SocketChannel channel = (SocketChannel) readableKey.channel();
            IPCHandle handle = (IPCHandle) readableKey.attachment();
            ByteBuffer readBuffer = handle.getInBuffer();
            try {
                int len = channel.read(readBuffer);
                if (len < 0) {
                    close(readableKey, channel);
                    return;
                }
                system.getPerformanceCounters().addMessageBytesReceived(len);
                handle.processIncomingMessages();
                if (!readBuffer.hasRemaining()) {
                    handle.resizeInBuffer();
                }
            } catch (IOException e) {
                LOGGER.error("TCP read error from {}", handle.getRemoteAddress(), e);
                close(readableKey, channel);
            }
        }

        private void write(SelectionKey writableKey) {
            SocketChannel channel = (SocketChannel) writableKey.channel();
            IPCHandle handle = (IPCHandle) writableKey.attachment();
            ByteBuffer writeBuffer = handle.getOutBuffer();
            try {
                int len = channel.write(writeBuffer);
                if (len < 0) {
                    close(writableKey, channel);
                    return;
                }
                system.getPerformanceCounters().addMessageBytesSent(len);
                if (!writeBuffer.hasRemaining()) {
                    writableKey.interestOps(writableKey.interestOps() & ~SelectionKey.OP_WRITE);
                }
                if (handle.full()) {
                    handle.clearFull();
                    selector.wakeup();
                }
            } catch (IOException e) {
                LOGGER.error("TCP write error to {}", handle.getRemoteAddress(), e);
                close(writableKey, channel);
            }
        }

        private void close(SelectionKey key, SocketChannel sc) {
            if (key != null) {
                final Object attachment = key.attachment();
                if (attachment != null) {
                    ((IPCHandle) attachment).close();
                }
                key.cancel();
            }
            if (sc != null) {
                NetworkUtil.closeQuietly(sc);
                openChannels.remove(sc);
            }
        }

        private void collectOutstandingWork() {
            synchronized (IPCConnectionManager.this) {
                if (!pendingConnections.isEmpty()) {
                    moveAll(pendingConnections, workingPendingConnections);
                }
                if (!sendList.isEmpty()) {
                    moveAll(sendList, workingSendList);
                }
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

        private <T> void moveAll(List<T> source, List<T> target) {
            target.addAll(source);
            source.clear();
        }
    }
}
