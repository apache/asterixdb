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
package org.apache.hyracks.control.nc.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.comm.IChannelControlBlock;
import org.apache.hyracks.api.comm.IChannelInterfaceFactory;
import org.apache.hyracks.api.comm.ICloseableBufferAcceptor;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.exceptions.NetException;
import org.apache.hyracks.api.network.ISocketChannelFactory;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.net.protocols.muxdemux.ChannelControlBlock;
import org.apache.hyracks.net.protocols.muxdemux.IChannelOpenListener;
import org.apache.hyracks.net.protocols.muxdemux.MultiplexedConnection;
import org.apache.hyracks.net.protocols.muxdemux.MuxDemux;
import org.apache.hyracks.net.protocols.muxdemux.MuxDemuxPerformanceCounters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MessagingNetworkManager {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int MAX_CONNECTION_ATTEMPTS = 5;
    private final MuxDemux md;
    private NetworkAddress localNetworkAddress;
    private NetworkAddress publicNetworkAddress;
    private final Map<String, IChannelControlBlock> ncChannels = new HashMap<>();
    private final NodeControllerService ncs;
    private final Map<IChannelControlBlock, ICloseableBufferAcceptor> channelFullBufferAcceptor = new HashMap<>();

    public MessagingNetworkManager(NodeControllerService ncs, String inetAddress, int inetPort, int nThreads,
            String publicInetAddress, int publicInetPort, IChannelInterfaceFactory channelInterfaceFactory,
            ISocketChannelFactory socketChannelFactory) {
        this.ncs = ncs;
        md = new MuxDemux(new InetSocketAddress(inetAddress, inetPort), new ChannelOpenListener(), nThreads,
                MAX_CONNECTION_ATTEMPTS, channelInterfaceFactory, socketChannelFactory);
        publicNetworkAddress = new NetworkAddress(publicInetAddress, publicInetPort);
    }

    public void start() throws IOException {
        md.start();
        InetSocketAddress sockAddr = md.getLocalAddress();
        localNetworkAddress = new NetworkAddress(sockAddr.getHostString(), sockAddr.getPort());

        // See if the public address was explicitly specified, and if not,
        // make it a copy of localNetworkAddress
        if (publicNetworkAddress.getAddress() == null) {
            publicNetworkAddress = localNetworkAddress;
        } else {
            // Likewise for public port
            if (publicNetworkAddress.getPort() == 0) {
                publicNetworkAddress = new NetworkAddress(publicNetworkAddress.getAddress(), sockAddr.getPort());
            }
        }
    }

    public void stop() {
        // Currently there is nothing to stop
    }

    public IChannelControlBlock getMessagingChannel(String nodeId) throws Exception {
        synchronized (ncChannels) {
            IChannelControlBlock ccb = ncChannels.get(nodeId);
            if (ccb == null) {
                // Establish new connection
                ccb = establishNewConnection(nodeId);
                addOpenChannel(nodeId, ccb);
            }
            return ccb;
        }
    }

    private ChannelControlBlock connect(SocketAddress remoteAddress) throws InterruptedException, NetException {
        MultiplexedConnection mConn = md.connect((InetSocketAddress) remoteAddress);
        return mConn.openChannel();
    }

    public MuxDemuxPerformanceCounters getPerformanceCounters() {
        return md.getPerformanceCounters();
    }

    public NetworkAddress getPublicNetworkAddress() {
        return publicNetworkAddress;
    }

    private void prepareMessagingInitialMessage(String ncId, final ByteBuffer buffer) throws NetException {
        /*
         * The messaging initial message contains the node id of the node
         * which requested the channel to be opened.
         */
        int intialMsgLength = Integer.BYTES + ncId.length();
        if (intialMsgLength > buffer.capacity()) {
            throw new NetException("Initial message exceded the channel buffer size " + buffer.capacity() + " bytes");
        }
        buffer.clear();
        buffer.putInt(ncId.length());
        buffer.put(ncId.getBytes());
        buffer.flip();
    }

    private IChannelControlBlock establishNewConnection(String nodeId) throws Exception {
        Map<String, NodeControllerInfo> nodeControllers = ncs.getNodeControllersInfo();

        // Get the node messaging address from its info
        NodeControllerInfo nodeControllerInfo = nodeControllers.get(nodeId);
        if (nodeControllerInfo == null) {
            throw new NetException("Could not find node: " + nodeId);
        }
        NetworkAddress nodeMessagingNeAddress = nodeControllerInfo.getMessagingNetworkAddress();
        SocketAddress nodeAddress = new InetSocketAddress(InetAddress.getByName(nodeMessagingNeAddress.getAddress()),
                nodeMessagingNeAddress.getPort());

        // Open the channel
        IChannelControlBlock ccb = connect(nodeAddress);
        try {
            // Prepare the initial message buffer
            ByteBuffer initialBuffer = ccb.getReadInterface().getBufferFactory().createBuffer();
            prepareMessagingInitialMessage(ncs.getId(), initialBuffer);
            // Send the initial messaging channel handshake message to register the opened channel on both nodes
            ccb.getWriteInterface().getFullBufferAcceptor().accept(initialBuffer);
            return ccb;
        } catch (NetException e) {
            closeChannel(ccb);
            throw e;
        }
    }

    private void addOpenChannel(final String nodeId, final IChannelControlBlock ccb) {
        synchronized (ncChannels) {
            if (ncChannels.get(nodeId) == null) {
                ncChannels.put(nodeId, ccb);
            } else {
                closeChannel(ccb);
                /*
                 * TODO Currently there is a chance that two nodes will open
                 * a channel to each other at the exact same time and both will
                 * end up using a half closed channel. While this isn't a big issue,
                 * it should be eliminated by introducing negotiation protocol
                 * between nodes to decide which channel to use and which channel
                 * to close fully.
                 */
            }
        }
    }

    private void closeChannel(IChannelControlBlock ccb) {
        ccb.getWriteInterface().getFullBufferAcceptor().close();
    }

    private class ChannelOpenListener implements IChannelOpenListener {
        @Override
        public void channelOpened(ChannelControlBlock channel) {
            // Store the channel's original acceptor (which is set by the application)
            ICloseableBufferAcceptor fullBufferAcceptor = channel.getReadInterface().getFullBufferAcceptor();
            synchronized (channelFullBufferAcceptor) {
                channelFullBufferAcceptor.put(channel, fullBufferAcceptor);
            }
            // Temporary set the acceptor to InitialBufferAcceptor to read the initial message
            channel.getReadInterface().setFullBufferAcceptor(new InitialBufferAcceptor(channel));
        }
    }

    private class InitialBufferAcceptor implements ICloseableBufferAcceptor {
        private final ChannelControlBlock ccb;

        public InitialBufferAcceptor(ChannelControlBlock ccb) {
            this.ccb = ccb;
        }

        @Override
        public void accept(ByteBuffer buffer) {
            String nodeId = readMessagingInitialMessage(buffer);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Opened messaging channel with node: " + nodeId);
            }
            // Return the channel's original acceptor
            ICloseableBufferAcceptor originalAcceptor;
            synchronized (channelFullBufferAcceptor) {
                originalAcceptor = channelFullBufferAcceptor.remove(ccb);
                if (originalAcceptor == null) {
                    throw new IllegalStateException("Could not find channel acceptor");
                }
            }
            ccb.getReadInterface().setFullBufferAcceptor(originalAcceptor);
            addOpenChannel(nodeId, ccb);
        }

        @Override
        public void close() {
            // Nothing to close
        }

        @Override
        public void error(int ecode) {
            // Errors should be handled in the application
        }

        private String readMessagingInitialMessage(ByteBuffer buffer) {
            int nodeIdLength = buffer.getInt();
            byte[] stringBytes = new byte[nodeIdLength];
            buffer.get(stringBytes);
            return new String(stringBytes);
        }
    }
}
