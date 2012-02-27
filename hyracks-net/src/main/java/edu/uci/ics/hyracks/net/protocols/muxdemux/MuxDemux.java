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
package edu.uci.ics.hyracks.net.protocols.muxdemux;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.net.exceptions.NetException;
import edu.uci.ics.hyracks.net.protocols.tcp.ITCPConnectionListener;
import edu.uci.ics.hyracks.net.protocols.tcp.TCPConnection;
import edu.uci.ics.hyracks.net.protocols.tcp.TCPEndpoint;

public class MuxDemux {
    private final InetSocketAddress localAddress;

    private final IChannelOpenListener channelOpenListener;

    private final Map<InetSocketAddress, MultiplexedConnection> connectionMap;

    private final TCPEndpoint tcpEndpoint;

    private final MuxDemuxPerformanceCounters perfCounters;

    public MuxDemux(InetSocketAddress localAddress, IChannelOpenListener listener, int nThreads) {
        this.localAddress = localAddress;
        this.channelOpenListener = listener;
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
                    if (nConnectionAttempts > 5) {
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

    public void start() throws IOException {
        tcpEndpoint.start(localAddress);
    }

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

    public InetSocketAddress getLocalAddress() {
        return tcpEndpoint.getLocalAddress();
    }

    public MuxDemuxPerformanceCounters getPerformanceCounters() {
        return perfCounters;
    }
}