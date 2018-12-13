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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IChannelInterfaceFactory;
import org.apache.hyracks.api.comm.ICloseableBufferAcceptor;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.exceptions.NetException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.network.ISocketChannelFactory;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.comm.channels.IChannelConnectionFactory;
import org.apache.hyracks.comm.channels.NetworkOutputChannel;
import org.apache.hyracks.control.nc.partitions.PartitionManager;
import org.apache.hyracks.net.protocols.muxdemux.ChannelControlBlock;
import org.apache.hyracks.net.protocols.muxdemux.IChannelOpenListener;
import org.apache.hyracks.net.protocols.muxdemux.MultiplexedConnection;
import org.apache.hyracks.net.protocols.muxdemux.MuxDemux;
import org.apache.hyracks.net.protocols.muxdemux.MuxDemuxPerformanceCounters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NetworkManager implements IChannelConnectionFactory {
    private static final Logger LOGGER = LogManager.getLogger();

    private static final int MAX_CONNECTION_ATTEMPTS = 5;

    static final int INITIAL_MESSAGE_SIZE = 20;

    private final PartitionManager partitionManager;

    private final int nBuffers;

    private final MuxDemux md;

    private NetworkAddress localNetworkAddress;

    private NetworkAddress publicNetworkAddress;

    public NetworkManager(String inetAddress, int inetPort, PartitionManager partitionManager, int nThreads,
            int nBuffers, String publicInetAddress, int publicInetPort,
            IChannelInterfaceFactory channelInterfaceFactory, ISocketChannelFactory socketChannelFactory) {
        this.partitionManager = partitionManager;
        this.nBuffers = nBuffers;
        md = new MuxDemux(new InetSocketAddress(inetAddress, inetPort), new ChannelOpenListener(), nThreads,
                MAX_CONNECTION_ATTEMPTS, channelInterfaceFactory, socketChannelFactory);
        // Just save these values for the moment; may be reset in start()
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

    public NetworkAddress getLocalNetworkAddress() {
        return localNetworkAddress;
    }

    public NetworkAddress getPublicNetworkAddress() {
        return publicNetworkAddress;
    }

    public void stop() {

    }

    @Override
    public ChannelControlBlock connect(SocketAddress remoteAddress) throws InterruptedException, NetException {
        MultiplexedConnection mConn = md.connect((InetSocketAddress) remoteAddress);
        return mConn.openChannel();
    }

    public MuxDemux getMuxDemux() {
        return md;
    }

    private class ChannelOpenListener implements IChannelOpenListener {
        @Override
        public void channelOpened(ChannelControlBlock channel) {
            channel.getReadInterface().setFullBufferAcceptor(new InitialBufferAcceptor(channel));
            channel.getReadInterface().getEmptyBufferAcceptor().accept(ByteBuffer.allocate(INITIAL_MESSAGE_SIZE));
        }
    }

    private class InitialBufferAcceptor implements ICloseableBufferAcceptor {
        private final ChannelControlBlock ccb;

        private NetworkOutputChannel noc;

        public InitialBufferAcceptor(ChannelControlBlock ccb) {
            this.ccb = ccb;
        }

        @Override
        public void accept(ByteBuffer buffer) {
            PartitionId pid = readInitialMessage(buffer);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Received initial partition request: " + pid + " on channel: " + ccb);
            }
            noc = new NetworkOutputChannel(ccb, nBuffers);
            partitionManager.registerPartitionRequest(pid, noc);
        }

        @Override
        public void close() {

        }

        @Override
        public void error(int ecode) {
            if (noc != null) {
                noc.abort(ecode);
            }
        }
    }

    private static PartitionId readInitialMessage(ByteBuffer buffer) {
        JobId jobId = new JobId(buffer.getLong());
        ConnectorDescriptorId cdid = new ConnectorDescriptorId(buffer.getInt());
        int senderIndex = buffer.getInt();
        int receiverIndex = buffer.getInt();
        return new PartitionId(jobId, cdid, senderIndex, receiverIndex);
    }

    public MuxDemuxPerformanceCounters getPerformanceCounters() {
        return md.getPerformanceCounters();
    }
}
