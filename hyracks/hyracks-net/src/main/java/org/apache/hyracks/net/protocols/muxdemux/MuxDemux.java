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
package edu.uci.ics.hyracks.net.protocols.muxdemux;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.net.exceptions.NetException;
import edu.uci.ics.hyracks.net.protocols.tcp.ITCPConnectionListener;
import edu.uci.ics.hyracks.net.protocols.tcp.TCPConnection;
import edu.uci.ics.hyracks.net.protocols.tcp.TCPEndpoint;

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

    private final Map<InetSocketAddress, MultiplexedConnection> connectionMap;

    private final TCPEndpoint tcpEndpoint;

    private final MuxDemuxPerformanceCounters perfCounters;

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
     */
    public MuxDemux(InetSocketAddress localAddress, IChannelOpenListener listener, int nThreads,
            int maxConnectionAttempts) {
        this.localAddress = localAddress;
        this.channelOpenListener = listener;
        this.maxConnectionAttempts = maxConnectionAttempts;
        connectionMap = new HashMap<InetSocketAddress, MultiplexedConnection>();
        this.tcpEndpoint = new TCPEndpoint(new ITCPConnectionListener() {
            @Override
            public void connectionEstablished(TCPConnection connection) {
                MultiplexedConnection mConn;
                synchronized (MuxDemux.this) {
                    mConn = connectionMap.get(connection.getRemoteAddress());
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
            }

            @Override
            public void connectionFailure(InetSocketAddress remoteAddress) {
                MultiplexedConnection mConn;
                synchronized (MuxDemux.this) {
                    mConn = connectionMap.get(remoteAddress);
                    assert mConn != null;
                    int nConnectionAttempts = mConn.getConnectionAttempts();
                    if (nConnectionAttempts > MuxDemux.this.maxConnectionAttempts) {
                        connectionMap.remove(remoteAddress);
                        mConn.setConnectionFailure();
                    } else {
                        mConn.setConnectionAttempts(nConnectionAttempts + 1);
                        tcpEndpoint.initiateConnection(remoteAddress);
                    }
                }
            }
        }, nThreads);
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
        MultiplexedConnection mConn = null;
        synchronized (this) {
            mConn = connectionMap.get(remoteAddress);
            if (mConn == null) {
                mConn = new MultiplexedConnection(this);
                connectionMap.put(remoteAddress, mConn);
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
}