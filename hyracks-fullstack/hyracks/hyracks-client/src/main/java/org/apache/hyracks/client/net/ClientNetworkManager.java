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
package org.apache.hyracks.client.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.apache.hyracks.api.exceptions.NetException;
import org.apache.hyracks.api.network.ISocketChannelFactory;
import org.apache.hyracks.comm.channels.IChannelConnectionFactory;
import org.apache.hyracks.net.protocols.muxdemux.ChannelControlBlock;
import org.apache.hyracks.net.protocols.muxdemux.FullFrameChannelInterfaceFactory;
import org.apache.hyracks.net.protocols.muxdemux.MultiplexedConnection;
import org.apache.hyracks.net.protocols.muxdemux.MuxDemux;
import org.apache.hyracks.net.protocols.muxdemux.MuxDemuxPerformanceCounters;

public class ClientNetworkManager implements IChannelConnectionFactory {
    private static final int MAX_CONNECTION_ATTEMPTS = 5;

    private final MuxDemux md;

    public ClientNetworkManager(int nThreads, ISocketChannelFactory socketChannelFactory) {
        /* This is a connect only socket and does not listen to any incoming connections, so pass null to
         * localAddress and listener.
         */
        md = new MuxDemux(null, null, nThreads, MAX_CONNECTION_ATTEMPTS, FullFrameChannelInterfaceFactory.INSTANCE,
                socketChannelFactory);
    }

    public void start() throws IOException {
        md.start();
    }

    public void stop() {

    }

    public ChannelControlBlock connect(SocketAddress remoteAddress) throws InterruptedException, NetException {
        MultiplexedConnection mConn = md.connect((InetSocketAddress) remoteAddress);
        return mConn.openChannel();
    }

    public MuxDemuxPerformanceCounters getPerformanceCounters() {
        return md.getPerformanceCounters();
    }
}
