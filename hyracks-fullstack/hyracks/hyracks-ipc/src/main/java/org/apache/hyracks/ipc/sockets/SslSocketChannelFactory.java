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
package org.apache.hyracks.ipc.sockets;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;

import org.apache.hyracks.api.network.INetworkSecurityManager;
import org.apache.hyracks.api.network.ISocketChannel;
import org.apache.hyracks.api.network.ISocketChannelFactory;
import org.apache.hyracks.util.annotations.AiProvenance;

public class SslSocketChannelFactory implements ISocketChannelFactory {

    private final INetworkSecurityManager networkSecurityManager;

    public SslSocketChannelFactory(INetworkSecurityManager networkSecurityManager) {
        this.networkSecurityManager = networkSecurityManager;
    }

    @Override
    public ISocketChannel createServerChannel(SocketChannel socketChannel) {
        final SSLEngine sslEngine = networkSecurityManager.newServerSSLEngine();
        return new SslSocketChannel(socketChannel, sslEngine);
    }

    @Override
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED, notes = "ASTERIXDB-3851: pass the dialled peer to the engine")
    public ISocketChannel createClientChannel(SocketChannel socketChannel) {
        final SSLEngine sslEngine = networkSecurityManager.newClientSSLEngine(peerOf(socketChannel));
        return new SslSocketChannel(socketChannel, sslEngine);
    }

    /**
     * The address the channel was connected to, which is the name the peer's certificate has to identify. This is
     * the address passed to {@link SocketChannel#connect}, not a re-resolution of it, so a peer dialled by hostname
     * is identified by that hostname.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "ASTERIXDB-3851")
    private static InetSocketAddress peerOf(SocketChannel socketChannel) {
        final SocketAddress remoteAddress;
        try {
            remoteAddress = socketChannel.getRemoteAddress();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (!(remoteAddress instanceof InetSocketAddress)) {
            // null when the channel is not connected; a client channel is only created once it is
            throw new IllegalStateException("cannot secure a client channel with remote address " + remoteAddress);
        }
        return (InetSocketAddress) remoteAddress;
    }
}
