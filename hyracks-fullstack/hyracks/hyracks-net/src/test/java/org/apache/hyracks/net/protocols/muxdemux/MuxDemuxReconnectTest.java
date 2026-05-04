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
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hyracks.ipc.sockets.PlainSocketChannelFactory;
import org.junit.Assert;
import org.junit.Test;

public class MuxDemuxReconnectTest {

    @Test
    public void testReconnectReplacesFailedCachedConnection() throws Exception {
        MuxDemux server = createMuxDemux();
        server.start();
        MuxDemux client = createMuxDemux();
        client.start();

        InetSocketAddress serverAddress = server.getLocalAddress();
        MultiplexedConnection firstConnection = client.connect(serverAddress);
        Assert.assertSame(firstConnection, getOutgoingConnection(client, serverAddress));

        firstConnection.setConnectionFailure(new IOException("forced connection failure"));

        MultiplexedConnection secondConnection = client.connect(serverAddress);
        Assert.assertNotSame("connect() should replace a failed cached connection", firstConnection, secondConnection);
        Assert.assertSame(secondConnection, getOutgoingConnection(client, serverAddress));
    }

    private static MuxDemux createMuxDemux() {
        return new MuxDemux(new InetSocketAddress("127.0.0.1", 0), channel -> {
            // This test only exercises connection caching and reconnect behavior.
        }, 1, 5, FullFrameChannelInterfaceFactory.INSTANCE, PlainSocketChannelFactory.INSTANCE);
    }

    @SuppressWarnings("unchecked")
    private static MultiplexedConnection getOutgoingConnection(MuxDemux muxDemux, InetSocketAddress remoteAddress)
            throws Exception {
        Field outgoingConnectionMapField = MuxDemux.class.getDeclaredField("outgoingConnectionMap");
        outgoingConnectionMapField.setAccessible(true);
        Map<InetSocketAddress, MultiplexedConnection> outgoingConnectionMap =
                (Map<InetSocketAddress, MultiplexedConnection>) outgoingConnectionMapField.get(muxDemux);
        return outgoingConnectionMap.get(remoteAddress);
    }
}