
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
package org.apache.asterix.metadata;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.rmi.server.RMIClientSocketFactory;

import javax.net.SocketFactory;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.hyracks.api.network.INetworkSecurityConfig;
import org.apache.hyracks.api.network.INetworkSecurityManager;
import org.apache.hyracks.ipc.security.NetworkSecurityManager;
import org.apache.hyracks.util.annotations.AiProvenance;

public class RMIClientFactory implements RMIClientSocketFactory, Serializable {

    private static final long serialVersionUID = -3874278041718817394L;
    private final INetworkSecurityConfig config;
    private transient SocketFactory socketFactory;

    private RMIClientFactory(INetworkSecurityConfig config) {
        this.config = config;

    }

    public static RMIClientSocketFactory getSocketFactory(INetworkSecurityManager securityManager) {
        // clients need to have the client factory on their classpath- to enable older clients, only use
        // our client socket factory when SSL is enabled
        INetworkSecurityConfig config = securityManager.getConfiguration();
        if (config.isSslEnabled()) {
            return new RMIClientFactory(config);
        }
        return null;
    }

    public Socket createSocket(String host, int port) throws IOException {
        synchronized (this) {
            if (socketFactory == null) {
                socketFactory = config.isSslEnabled() ? new RMITrustedClientSSLSocketFactory(config)
                        : SocketFactory.getDefault();
            }
        }
        return socketFactory.createSocket(host, port);
    }

    private static class RMITrustedClientSSLSocketFactory extends SSLSocketFactory {

        protected SSLSocketFactory factory;
        private final boolean verifyPeerIdentity;

        public RMITrustedClientSSLSocketFactory(INetworkSecurityConfig config) {
            this.factory = NetworkSecurityManager.newSSLContext(config, false).getSocketFactory();
            this.verifyPeerIdentity = config.verifyRmiPeerIdentity();
        }

        public Socket createSocket(InetAddress host, int port) throws IOException {
            return identifyPeer(this.factory.createSocket(host, port));
        }

        public Socket createSocket(String host, int port) throws IOException {
            return identifyPeer(this.factory.createSocket(host, port));
        }

        public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
            return identifyPeer(this.factory.createSocket(host, port, localHost, localPort));
        }

        public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
                throws IOException {
            return identifyPeer(this.factory.createSocket(address, port, localAddress, localPort));
        }

        public Socket createSocket(Socket socket, String host, int port, boolean autoClose) throws IOException {
            return identifyPeer(this.factory.createSocket(socket, host, port, autoClose));
        }

        /**
         * Requires the certificate the peer presents to identify the host this socket was opened to. An
         * {@link SSLSocket} performs no such check unless it is asked to, leaving any holder of a certificate the
         * trust store chains to able to answer in place of the metadata node.
         */
        @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "ASTERIXDB-3851")
        private Socket identifyPeer(Socket socket) {
            if (!verifyPeerIdentity) {
                return socket;
            }
            SSLSocket sslSocket = (SSLSocket) socket;
            SSLParameters sslParameters = sslSocket.getSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm(NetworkSecurityManager.ENDPOINT_IDENTIFICATION_ALGORITHM);
            sslSocket.setSSLParameters(sslParameters);
            return sslSocket;
        }

        public String[] getDefaultCipherSuites() {
            return this.factory.getDefaultCipherSuites();
        }

        public String[] getSupportedCipherSuites() {
            return this.factory.getSupportedCipherSuites();
        }
    }
}
