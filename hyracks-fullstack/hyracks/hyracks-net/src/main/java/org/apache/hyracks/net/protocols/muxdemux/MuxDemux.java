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
package org.apache.hyracks.net.protocols.muxdemux;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.comm.IChannelInterfaceFactory;
import org.apache.hyracks.api.exceptions.NetException;
import org.apache.hyracks.api.network.ISocketChannelFactory;
import org.apache.hyracks.net.protocols.tcp.ITCPConnectionListener;
import org.apache.hyracks.net.protocols.tcp.TCPConnection;
import org.apache.hyracks.net.protocols.tcp.TCPEndpoint;
import org.apache.hyracks.util.JSONUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Multiplexed Connection Manager.
 * Every participant that wants to use the multiplexed connections must create and instance
 * of this class.
 *
 * @author vinayakb
 */
public class MuxDemux {
    private final InetSocketAddress localAddress;

    private final IChannelOpenListener channelOpenListener;

    private final int maxConnectionAttempts;

    private final Map<InetSocketAddress, MultiplexedConnection> outgoingConnectionMap;
    private final Map<InetSocketAddress, MultiplexedConnection> incomingConnectionMap;

    private final TCPEndpoint tcpEndpoint;

    private final MuxDemuxPerformanceCounters perfCounters;

    private final IChannelInterfaceFactory channelInterfaceFatory;

    /**
     * Constructor.
     *
     * @param localAddress
     *            - TCP/IP socket address to listen on. Null for non-listening unidirectional sockets
     * @param listener
     *            - Callback interface to report channel events. Null for non-listening unidirectional sockets
     * @param nThreads
     *            - Number of threads to use for data transfer
     * @param maxConnectionAttempts
     *            - Maximum number of connection attempts
     * @param channelInterfaceFactory
     *            - The channel interface factory
     * @param socketChannelFactory
     *            - The socket channel factory
     */
    public MuxDemux(InetSocketAddress localAddress, IChannelOpenListener listener, int nThreads,
            int maxConnectionAttempts, IChannelInterfaceFactory channelInterfaceFactory,
            ISocketChannelFactory socketChannelFactory) {
        this.localAddress = localAddress;
        this.channelOpenListener = listener;
        this.maxConnectionAttempts = maxConnectionAttempts;
        this.channelInterfaceFatory = channelInterfaceFactory;
        outgoingConnectionMap = new HashMap<>();
        incomingConnectionMap = new HashMap<>();
        this.tcpEndpoint = new TCPEndpoint(new ITCPConnectionListener() {
            @Override
            public void connectionEstablished(TCPConnection connection) {
                MultiplexedConnection mConn;
                synchronized (MuxDemux.this) {
                    mConn = outgoingConnectionMap.get(connection.getRemoteAddress());
                }
                assert mConn != null;
                mConn.setTCPConnection(connection);
                connection.setEventListener(mConn);
                connection.setAttachment(mConn);
            }

            @Override
            public void acceptedConnection(TCPConnection connection) {
                MultiplexedConnection mConn = new MultiplexedConnection(MuxDemux.this);
                mConn.setTCPConnection(connection);
                connection.setEventListener(mConn);
                connection.setAttachment(mConn);
                incomingConnectionMap.put(connection.getRemoteAddress(), mConn);
            }

            @Override
            public void connectionFailure(InetSocketAddress remoteAddress, IOException error) {
                MultiplexedConnection mConn;
                synchronized (MuxDemux.this) {
                    mConn = outgoingConnectionMap.get(remoteAddress);
                    assert mConn != null;
                    int nConnectionAttempts = mConn.getConnectionAttempts();
                    if (nConnectionAttempts > MuxDemux.this.maxConnectionAttempts) {
                        outgoingConnectionMap.remove(remoteAddress);
                        mConn.setConnectionFailure(new IOException(remoteAddress.toString() + ": " + error, error));
                    } else {
                        mConn.setConnectionAttempts(nConnectionAttempts + 1);
                        tcpEndpoint.initiateConnection(remoteAddress);
                    }
                }
            }

            @Override
            public void connectionClosed(TCPConnection connection) {
                synchronized (MuxDemux.this) {
                    if (connection.getType() == TCPConnection.ConnectionType.OUTGOING) {
                        outgoingConnectionMap.remove(connection.getRemoteAddress());
                    } else if (connection.getType() == TCPConnection.ConnectionType.INCOMING) {
                        incomingConnectionMap.remove(connection.getRemoteAddress());
                    }
                }
            }
        }, nThreads, socketChannelFactory);
        perfCounters = new MuxDemuxPerformanceCounters();
    }

    /**
     * Starts listening for remote connections and is capable of initiating connections.
     *
     * @throws IOException
     */
    public void start() throws IOException {
        tcpEndpoint.start(localAddress);
    }

    /**
     * Create a {@link MultiplexedConnection} that can create channels to the specified remote address.
     * The remote address must have a {@link MuxDemux} listening for connections.
     *
     * @param remoteAddress
     *            - Address of the remote {@link MuxDemux}
     * @return a {@link MultiplexedConnection} connected to the remote address.
     * @throws InterruptedException
     *             - This call was interrupted while waiting for connection setup.
     *             In such an event, it is safe to retry the {@link #connect(InetSocketAddress)} call.
     * @throws NetException
     */
    public MultiplexedConnection connect(InetSocketAddress remoteAddress) throws InterruptedException, NetException {
        MultiplexedConnection mConn;
        synchronized (this) {
            mConn = outgoingConnectionMap.get(remoteAddress);
            if (mConn == null) {
                mConn = new MultiplexedConnection(this);
                outgoingConnectionMap.put(remoteAddress, mConn);
                tcpEndpoint.initiateConnection(remoteAddress);
            }
        }
        mConn.waitUntilConnected();
        return mConn;
    }

    IChannelOpenListener getChannelOpenListener() {
        return channelOpenListener;
    }

    /**
     * Get the local address that this {@link MuxDemux} is listening for connections.
     *
     * @return local TCP/IP socket address.
     */
    public InetSocketAddress getLocalAddress() {
        return tcpEndpoint.getLocalAddress();
    }

    /**
     * Gets performance counters useful for collecting efficiency metrics.
     *
     * @return
     */
    public MuxDemuxPerformanceCounters getPerformanceCounters() {
        return perfCounters;
    }

    /**
     * Gets the channel interface factory associated with channels
     * created by this {@link MuxDemux}.
     *
     * @return
     */
    public IChannelInterfaceFactory getChannelInterfaceFactory() {
        return channelInterfaceFatory;
    }

    public synchronized JsonNode getState() {
        final ObjectNode state = JSONUtil.createObject();
        state.put("localAddress", tcpEndpoint.getLocalAddress().toString());
        final ArrayNode outgoingConnections = JSONUtil.createArray();
        state.set("outgoingConnections", outgoingConnections);
        for (MultiplexedConnection connection : outgoingConnectionMap.values()) {
            connection.getState().ifPresent(outgoingConnections::add);
        }
        final ArrayNode incomingConnections = JSONUtil.createArray();
        state.set("incomingConnections", incomingConnections);
        for (MultiplexedConnection connection : incomingConnectionMap.values()) {
            connection.getState().ifPresent(incomingConnections::add);
        }
        return state;
    }
}
