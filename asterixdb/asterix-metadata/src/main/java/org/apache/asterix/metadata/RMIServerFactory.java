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
import java.net.InetAddress;
import java.net.ServerSocket;
import java.rmi.server.RMIServerSocketFactory;
import java.util.Optional;

import javax.net.ServerSocketFactory;

import org.apache.hyracks.api.network.INetworkSecurityManager;

public class RMIServerFactory implements RMIServerSocketFactory {

    // default backlog used by the JDK (e.g. sun.security.ssl.SSLServerSocketFactoryImpl)
    private static final int DEFAULT_BACKLOG = 50;
    private final INetworkSecurityManager securityManager;

    private RMIServerFactory(INetworkSecurityManager securityManager) {
        this.securityManager = securityManager;
    }

    public static RMIServerSocketFactory getSocketFactory(INetworkSecurityManager securityManager) {
        return new RMIServerFactory(securityManager);
    }

    @Override
    public ServerSocket createServerSocket(int port) throws IOException {
        ServerSocketFactory socketFactory;
        if (securityManager.getConfiguration().isSslEnabled()) {
            socketFactory = securityManager.newSSLContext().getServerSocketFactory();
        } else {
            socketFactory = ServerSocketFactory.getDefault();
        }
        Optional<InetAddress> rmiBindAddress = securityManager.getConfiguration().getRMIBindAddress();
        if (rmiBindAddress.isPresent()) {
            return socketFactory.createServerSocket(port, DEFAULT_BACKLOG, rmiBindAddress.get());
        } else {
            return socketFactory.createServerSocket(port);
        }
    }
}
