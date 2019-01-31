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

package org.apache.asterix.metadata.bootstrap;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.metadata.RMIClientFactory;
import org.apache.asterix.metadata.RMIServerFactory;
import org.apache.asterix.metadata.api.IAsterixStateProxy;
import org.apache.asterix.metadata.api.IMetadataNode;
import org.apache.hyracks.api.network.INetworkSecurityManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Contains Asterix distributed state such as the AsterixProperties.
 */
public class AsterixStateProxy implements IAsterixStateProxy {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();

    private IMetadataNode metadataNode;
    private static final IAsterixStateProxy cc = new AsterixStateProxy();

    public static IAsterixStateProxy registerRemoteObject(INetworkSecurityManager networkSecurityManager,
            int metadataCallbackPort) throws RemoteException {
        IAsterixStateProxy stub;
        // clients need to have the client factory on their classpath- to enable older clients, only use
        // our client socket factory when SSL is enabled
        if (networkSecurityManager.getConfiguration().isSslEnabled()) {
            final RMIServerFactory serverSocketFactory = new RMIServerFactory(networkSecurityManager);
            final RMIClientFactory clientSocketFactory =
                    new RMIClientFactory(networkSecurityManager.getConfiguration().isSslEnabled());
            stub = (IAsterixStateProxy) UnicastRemoteObject.exportObject(cc, metadataCallbackPort, clientSocketFactory,
                    serverSocketFactory);
        } else {
            stub = (IAsterixStateProxy) UnicastRemoteObject.exportObject(cc, metadataCallbackPort);
        }
        LOGGER.info("Asterix Distributed State Proxy Bound");
        return stub;
    }

    public static void unregisterRemoteObject() throws RemoteException {
        UnicastRemoteObject.unexportObject(cc, true);
        LOGGER.info("Asterix Distributed State Proxy Unbound");
    }

    @Override
    public synchronized void setMetadataNode(IMetadataNode metadataNode) {
        this.metadataNode = metadataNode;
        notifyAll();
    }

    @Override
    public IMetadataNode waitForMetadataNode(long waitFor, TimeUnit timeUnit) throws InterruptedException {
        synchronized (this) {
            //TODO(mblow): replace with nanoTime() to avoid being affected by system clock adjustments...
            long timeToWait = TimeUnit.MILLISECONDS.convert(waitFor, timeUnit);
            while (metadataNode == null && timeToWait > 0) {
                long startTime = System.currentTimeMillis();
                wait(timeToWait);
                timeToWait -= System.currentTimeMillis() - startTime;
            }
            return metadataNode;
        }
    }
}
